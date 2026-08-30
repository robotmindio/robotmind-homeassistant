"""Outbound WebSocket bridge; Home Assistant is never exposed to Fiber."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from aiohttp import ClientError, WSMsgType
from homeassistant.components.recorder import history
from homeassistant.core import Event, HomeAssistant, ServiceCallTarget
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store

from .const import CONF_CONNECTION_ID, CONF_SECRET


class BridgeError(RuntimeError):
    pass


_MAX_EVENT_BYTES = 65_536
_MAX_RESULT_BYTES = 262_144
_MAX_ENTITIES = 500
_MAX_HISTORY_STATES = 500


def _bridge_url(base_url: str) -> str:
    parts = urlsplit(base_url.rstrip("/"))
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, "/integrations/homeassistant/bridge", "", ""))


class Bridge:
    def __init__(self, hass: HomeAssistant, data: dict[str, Any]) -> None:
        self.hass = hass
        self.data = data
        self._task: asyncio.Task[None] | None = None
        self._ws: Any = None
        self._unsub_state: Any = None
        self._subscriptions: list[dict[str, Any]] = []
        self._watched_entities: set[str] = set()
        self._store = Store(hass, 1, f"robotmind_fiber.{data[CONF_CONNECTION_ID]}")
        self._completed: dict[str, dict[str, Any]] = {}

    @classmethod
    async def pair(cls, hass: HomeAssistant, base_url: str, code: str) -> dict[str, str]:
        try:
            async with async_get_clientsession(hass).ws_connect(
                _bridge_url(base_url), heartbeat=20, max_msg_size=_MAX_EVENT_BYTES
            ) as ws:
                await ws.send_json({"type": "pair", "code": code})
                message = await ws.receive(timeout=15)
                if message.type is not WSMsgType.TEXT:
                    raise BridgeError("pairing_failed")
                payload = json.loads(message.data)
                if not isinstance(payload, dict) or payload.get("type") != "paired":
                    raise BridgeError("pairing_failed")
                return {
                    CONF_CONNECTION_ID: payload[CONF_CONNECTION_ID],
                    CONF_SECRET: payload[CONF_SECRET],
                }
        except (ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError) as exc:
            raise BridgeError("pairing_failed") from exc

    @staticmethod
    def _bounded_result(result: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(result, default=str).encode()) > _MAX_RESULT_BYTES:
            return {"error": "response_too_large"}
        return result

    @staticmethod
    def _bounded_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(attributes, default=str).encode()) > _MAX_EVENT_BYTES // 2:
            return {}
        return attributes

    def _set_subscriptions(self, subscriptions: Any) -> None:
        self._subscriptions = [item for item in subscriptions if isinstance(item, dict)]
        self._watched_entities = {
            entity_id
            for subscription in self._subscriptions
            for entity_id in subscription.get("entity_ids", [])
            if isinstance(entity_id, str)
        }

    async def async_start(self) -> None:
        from homeassistant.const import EVENT_STATE_CHANGED

        self._completed = await self._store.async_load() or {}
        self._unsub_state = self.hass.bus.async_listen(EVENT_STATE_CHANGED, self._state_changed)
        self._task = self.hass.async_create_task(self._run())

    async def async_stop(self) -> None:
        if self._unsub_state:
            self._unsub_state()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def emit(self, event_type: str, data: dict[str, Any], context_id: str = "") -> None:
        await self._send(
            {
                "type": "event",
                "event_type": "event",
                "data": {"event_type": event_type, "data": data, "context_id": context_id or uuid.uuid4().hex},
            }
        )

    async def _state_changed(self, event: Event) -> None:
        old_state = event.data.get("old_state")
        new_state = event.data.get("new_state")
        if new_state is None:
            return
        if new_state.entity_id not in self._watched_entities:
            return
        await self._send(
            {
                "type": "event",
                "event_type": "state_changed",
                "data": {
                    "entity_id": new_state.entity_id,
                    "old_state": old_state.state if old_state else None,
                    "new_state": new_state.state,
                    "attributes": self._bounded_attributes(dict(new_state.attributes)),
                    "context_id": event.context.id,
                },
            }
        )

    async def _run(self) -> None:
        delay = 1
        while True:
            try:
                async with async_get_clientsession(self.hass).ws_connect(
                    _bridge_url(self.data["fiber_url"]),
                    heartbeat=20,
                    max_msg_size=_MAX_EVENT_BYTES,
                ) as ws:
                    self._ws = ws
                    await ws.send_json({"type": "auth", "connection_id": self.data[CONF_CONNECTION_ID], "secret": self.data[CONF_SECRET]})
                    auth = await ws.receive(timeout=15)
                    if (
                        auth.type is not WSMsgType.TEXT
                        or json.loads(auth.data).get("type") != "authenticated"
                    ):
                        raise BridgeError("authentication_failed")
                    delay = 1
                    async for message in ws:
                        if message.type is WSMsgType.TEXT:
                            payload = json.loads(message.data)
                            if not isinstance(payload, dict):
                                continue
                            if payload.get("type") == "configure":
                                self._set_subscriptions(payload.get("subscriptions", []))
                            elif (
                                payload.get("type") == "command"
                                and isinstance(payload.get("id"), str)
                                and isinstance(payload.get("command"), str)
                                and isinstance(payload.get("payload", {}), dict)
                            ):
                                result = await self._command(
                                    payload["id"], payload["command"], payload.get("payload", {})
                                )
                                await ws.send_json({"type": "result", "id": payload["id"], "result": result})
            except asyncio.CancelledError:
                raise
            except (
                BridgeError,
                ClientError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
                KeyError,
                TypeError,
            ):
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            finally:
                self._ws = None

    async def _send(self, payload: dict[str, Any]) -> None:
        if len(json.dumps(payload, default=str).encode()) > _MAX_EVENT_BYTES:
            raise BridgeError("event_too_large")
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_json(payload)

    async def _command(self, request_id: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if request_id in self._completed:
            return self._completed[request_id]
        try:
            if command == "get_state":
                state = self.hass.states.get(payload["entity_id"])
                return self._bounded_result({"state": state.as_dict() if state else None})
            if command == "list_entities":
                states = self.hass.states.async_all()
                return self._bounded_result(
                    {
                        "entities": [state.as_dict() for state in states[:_MAX_ENTITIES]],
                        "truncated": len(states) > _MAX_ENTITIES,
                    }
                )
            if command == "list_services":
                return self._bounded_result(
                    {
                        "services": {
                            domain: sorted(services)
                            for domain, services in self.hass.services.async_services().items()
                        }
                    }
                )
            if command == "get_history":
                raw_start = payload.get("start")
                start = (
                    datetime.fromisoformat(raw_start)
                    if isinstance(raw_start, str) and raw_start
                    else datetime.now(UTC) - timedelta(days=1)
                )
                if start.tzinfo is None:
                    start = start.replace(tzinfo=UTC)
                states = await self.hass.async_add_executor_job(
                    partial(
                        history.get_significant_states,
                        self.hass,
                        start_time=start,
                        end_time=datetime.now(UTC),
                        entity_ids={payload["entity_id"]},
                        include_start_time_state=True,
                        significant_changes_only=True,
                    )
                )
                entity_states = states.get(payload["entity_id"], [])
                return self._bounded_result(
                    {
                        "history": [state.as_dict() for state in entity_states[:_MAX_HISTORY_STATES]],
                        "truncated": len(entity_states) > _MAX_HISTORY_STATES,
                    }
                )
            if command == "fire_event":
                self.hass.bus.async_fire(payload["event_type"], payload.get("data", {}))
                return await self._remember(request_id, {"fired": payload["event_type"]})
            if command == "call_service":
                target = payload.get("target", {})
                if not isinstance(target, dict) or not isinstance(payload.get("data", {}), dict):
                    return {"error": "invalid_service_payload"}
                await self.hass.services.async_call(
                    payload["domain"],
                    payload["service"],
                    payload.get("data", {}),
                    target=ServiceCallTarget(
                        entity_id=target.get("entity_id"), device_id=target.get("device_id"), area_id=target.get("area_id")
                    ),
                    blocking=True,
                )
                return await self._remember(request_id, {"called": f"{payload['domain']}.{payload['service']}"})
            return {"error": "unknown_command"}
        except (HomeAssistantError, KeyError, OSError, TypeError, ValueError) as exc:
            result = {"error": f"{type(exc).__name__}:{exc}"}
            return await self._remember(request_id, result) if command in {"call_service", "fire_event"} else result

    async def _remember(self, request_id: str, result: dict[str, Any]) -> dict[str, Any]:
        self._completed[request_id] = result
        while len(self._completed) > 1_000:
            self._completed.pop(next(iter(self._completed)))
        await self._store.async_save(self._completed)
        return result
