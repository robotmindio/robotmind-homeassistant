"""RobotMind Fiber Home Assistant integration."""

from __future__ import annotations

import voluptuous as vol
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv

from .bridge import Bridge
from .const import DATA_BRIDGES, DOMAIN, SERVICE_EMIT


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    bridge = Bridge(hass, dict(entry.data))
    bridges = hass.data.setdefault(DATA_BRIDGES, {})
    bridges[entry.entry_id] = bridge
    await bridge.async_start()
    if not hass.services.has_service(DOMAIN, SERVICE_EMIT):
        async def emit(call: ServiceCall) -> None:
            for active in hass.data[DATA_BRIDGES].values():
                await active.emit(call.data["event_type"], dict(call.data.get("data", {})), call.context.id)
        hass.services.async_register(
            DOMAIN,
            SERVICE_EMIT,
            emit,
            schema=vol.Schema({vol.Required("event_type"): cv.string, vol.Optional("data", default={}): dict}),
        )
    return True


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    bridge = hass.data[DATA_BRIDGES].pop(entry.entry_id)
    await bridge.async_stop()
    if not hass.data[DATA_BRIDGES]:
        hass.services.async_remove(DOMAIN, SERVICE_EMIT)
    return True
