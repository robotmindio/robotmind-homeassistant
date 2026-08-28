"""Outbound WebSocket bridge; Home Assistant is never exposed to Fiber."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from aiohttp import WSMsgType
from homeassistant.components.recorder import history
from homeassistant.core import Event, HomeAssistant, ServiceCallTarget
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store

from .const import CONF_CONNECTION_ID, CONF_SECRET


class BridgeError(RuntimeError):
    pass


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
        self._store = Store(hass, 1, f"robotmind_fiber.{data[CONF_CONNECTION_ID]}")
        self._completed: dict[str, dict[str, Any]] = {}

    @classmethod
    async def pair(cls, hass: HomeAssistant, base_url: str, code: str) -> dict[str, str]:
        async with async_get_clientsession(hass).ws_connect(_bridge_url(base_url), heartbeat=20) as ws:
            await ws.send_json({"type": "pair", "code": code})
            message = await ws.receive(timeout=15)
            if message.type is not WSMsgType.TEXT:
                raise BridgeError("pairing_failed")
            payload = json.loads(message.data)
            if payload.get("type") != "paired":
                raise BridgeError("pairing_failed")
            return {CONF_CONNECTION_ID: payload[CONF_CONNECTION_ID], CONF_SECRET: payload[CONF_SECRET]}

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
        await self._send(
            {
                "type": "event",
                "event_type": "state_changed",
                "data": {
                    "entity_id": new_state.entity_id,
                    "old_state": old_state.state if old_state else None,
                    "new_state": new_state.state,
                    "attributes": dict(new_state.attributes),
                    "context_id": event.context.id,
                },
            }
        )

    async def _run(self) -> None:
        delay = 1
        while True:
            try:
                async with async_get_clientsession(self.hass).ws_connect(_bridge_url(self.data["fiber_url"]), heartbeat=20) as ws:
                    self._ws = ws
                    await ws.send_json({"type": "auth", "connection_id": self.data[CONF_CONNECTION_ID], "secret": self.data[CONF_SECRET]})
                    auth = await ws.receive(timeout=15)
                    if auth.type is not WSMsgType.TEXT or json.loads(auth.data).get("type") != "authenticated":
                        raise BridgeError("authentication_failed")
                    delay = 1
                    async for message in ws:
                        if message.type is WSMsgType.TEXT:
                            payload = json.loads(message.data)
                            if payload.get("type") == "command":
                                result = await self._command(payload["id"], payload["command"], payload.get("payload", {}))
                                await ws.send_json({"type": "result", "id": payload["id"], "result": result})
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            finally:
                self._ws = None

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_json(payload)

    async def _command(self, request_id: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if request_id in self._completed:
            return self._completed[request_id]
        try:
            if command == "get_state":
                state = self.hass.states.get(payload["entity_id"])
                result = {"state": state.as_dict() if state else None}
                return await self._remember(request_id, result)
            if command == "list_entities":
                return await self._remember(request_id, {"entities": [state.as_dict() for state in self.hass.states.async_all()]})
            if command == "list_services":
                return await self._remember(request_id, {"services": {domain: sorted(services) for domain, services in self.hass.services.async_services().items()}})
            if command == "get_history":
                states = await self.hass.async_add_executor_job(
                    history.get_significant_states,
                    self.hass,
                    datetime.fromisoformat(payload["start"]),
                    None,
                    {payload["entity_id"]},
                    None,
                    True,
                    True,
                )
                return await self._remember(request_id, {"history": [state.as_dict() for state in states.get(payload["entity_id"], [])]})
            if command == "fire_event":
                self.hass.bus.async_fire(payload["event_type"], payload.get("data", {}))
                return await self._remember(request_id, {"fired": payload["event_type"]})
            if command == "call_service":
                target = payload.get("target", {})
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
            return await self._remember(request_id, {"error": "unknown_command"})
        except Exception as exc:
            return await self._remember(request_id, {"error": f"{type(exc).__name__}:{exc}"})

    async def _remember(self, request_id: str, result: dict[str, Any]) -> dict[str, Any]:
        self._completed[request_id] = result
        while len(self._completed) > 1_000:
            self._completed.pop(next(iter(self._completed)))
        await self._store.async_save(self._completed)
        return result
