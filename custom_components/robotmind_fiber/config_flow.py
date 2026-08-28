"""Config flow for a one-time Fiber pairing code."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant import config_entries

from .bridge import Bridge, BridgeError
from .const import CONF_CONNECTION_ID, CONF_FIBER_URL, CONF_PAIRING_CODE, CONF_SECRET, DOMAIN


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                paired = await Bridge.pair(
                    self.hass, user_input[CONF_FIBER_URL], user_input[CONF_PAIRING_CODE]
                )
            except BridgeError:
                errors["base"] = "cannot_pair"
            else:
                await self.async_set_unique_id(paired[CONF_CONNECTION_ID])
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="RobotMind Fiber",
                    data={CONF_FIBER_URL: user_input[CONF_FIBER_URL], **paired},
                )
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_FIBER_URL, default="https://fiber-api.robotmind.io"): str,
                    vol.Required(CONF_PAIRING_CODE): str,
                }
            ),
            errors=errors,
        )
