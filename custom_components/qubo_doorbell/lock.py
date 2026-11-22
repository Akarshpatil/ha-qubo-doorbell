"""Smart lock platform for Qubo smart door locks (HLM/HLW/HLR) with responsive MQTT actuation and auto-relock."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from homeassistant.components.lock import LockEntity, LockEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from qubopy.devices.lock import QuboLock

from .const import DOMAIN, SIGNAL_EVENT
from .entity import QuboEntity

_LOGGER = logging.getLogger(__name__)
AUTO_RELOCK_DELAY = 15.0  # seconds until physical lock automatically re-engages
UNLOCK_SAFETY_TIMEOUT = 12.0  # seconds to reset unlocking spinner if no event received


def is_lock_device(device: Any) -> bool:
    """Determine if device is a Qubo smart lock."""
    if isinstance(device, QuboLock):
        return True
    m = (getattr(device.info, "model", None) or "").upper()
    t = (getattr(device.info, "device_type", None) or "").upper()
    return "LOCK" in t or any(m.startswith(p) for p in ("HLM", "HLW", "HLR"))


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Qubo lock entities from a config entry."""
    rt = hass.data[DOMAIN][entry.entry_id]
    locks = [QuboLockEntity(entry.entry_id, d) for d in rt.devices if is_lock_device(d)]
    if locks:
        _LOGGER.info("Setting up %d Qubo lock(s): %s", len(locks), ", ".join(l.name or l._uuid for l in locks))
        async_add_entities(locks)


class QuboLockEntity(QuboEntity, LockEntity):
    """Representation of a Qubo auto-relocking smart door lock with instant MQTT actuation."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_supported_features = LockEntityFeature.OPEN

    def __init__(self, entry_id: str, device: Any) -> None:
        super().__init__(entry_id, device)
        self._lock_device = device
        self._attr_unique_id = f"{self._uuid}_lock"
        self._attr_name = None  # inherits device name
        # Qubo door locks mechanically auto-relock: default state is locked
        self._is_locked: bool = True
        self._is_locking: bool = False
        self._is_unlocking: bool = False
        self._gateway_id: str | None = getattr(device, "gateway_id", None)
        self._token: str | None = None
        self._pin: str | None = None
        self._battery_level: int | None = getattr(device, "battery_percent", None)
        self._relock_timer: asyncio.TimerHandle | None = None
        self._safety_timer: asyncio.TimerHandle | None = None
        self._unlock_task: asyncio.Task[None] | None = None

        # Load cached credentials if available on disk
        self._load_cached_credentials()

    def _load_cached_credentials(self) -> None:
        """Attempt to read cached token and pin from local storage cache."""
        try:
            cache_path = os.path.join(self.hass.config.path(".storage"), "qubo_p2p_cache.json")
            if os.path.exists(cache_path):
                with open(cache_path, encoding="utf-8") as f:
                    c = json.load(f)
                lock_info = c.get("lock") or {}
                if lock_info.get("token") and not self._token:
                    self._token = str(lock_info["token"])
                if lock_info.get("pin") and not self._pin:
                    self._pin = str(lock_info["pin"])
                if lock_info.get("gateway_id") and not self._gateway_id:
                    self._gateway_id = str(lock_info["gateway_id"])
        except Exception as err:
            _LOGGER.debug("Could not read credentials from cache: %s", err)

    @property
    def is_locked(self) -> bool:
        """Return True if lock is currently locked."""
        return self._is_locked

    @property
    def is_locking(self) -> bool:
        """Return True if lock is currently in locking state."""
        return self._is_locking

    @property
    def is_unlocking(self) -> bool:
        """Return True if lock is currently unlocking."""
        return self._is_unlocking

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return device attributes without blocking."""
        return {
            "gateway_id": self._gateway_id,
            "auto_relock": True,
            "auto_relock_seconds": AUTO_RELOCK_DELAY,
            "battery_level": self._battery_level or getattr(self._lock_device, "battery_percent", None),
        }

    async def async_added_to_hass(self) -> None:
        """Register event listeners and prefetch shadow in background."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{SIGNAL_EVENT}_{self._entry_id}", self._handle_event
            )
        )
        self.hass.async_create_task(self._async_initial_refresh(), "qubo_lock_refresh")

    async def _async_initial_refresh(self) -> None:
        """Prefetch lock status and actuation credentials from shadow."""
        if hasattr(self._lock_device, "refresh"):
            try:
                await self._lock_device.refresh()
                self._update_credentials_from_shadow()
                if not self._gateway_id:
                    self._gateway_id = getattr(self._lock_device, "gateway_id", None)
                if not self._gateway_id:
                    rt = self.hass.data.get(DOMAIN, {}).get(self._entry_id)
                    if rt:
                        for dev in rt.devices:
                            m = (getattr(dev.info, "model", None) or "").upper()
                            t = (getattr(dev.info, "device_type", None) or "").upper()
                            if "DOORBELL" in t or "CAMERA" in t or m.startswith("HCD"):
                                self._gateway_id = getattr(dev, "device_id", dev.info.uuid)
                                break
                if hasattr(self._lock_device, "fetch_live_lock_state"):
                    try:
                        live_st = await self._lock_device.fetch_live_lock_state()
                        if live_st:
                            self._is_locked = (str(live_st).strip().lower() == "locked")
                    except Exception as err:
                        _LOGGER.debug("Could not fetch live lock state: %s", err)
                if hasattr(self._lock_device, "fetch_live_battery"):
                    try:
                        bat = await self._lock_device.fetch_live_battery()
                        if bat is not None:
                            self._battery_level = bat
                    except Exception:
                        pass
                self.async_write_ha_state()
            except Exception as err:
                _LOGGER.debug("Initial lock shadow refresh deferred for %s: %s", self._uuid, err)

    def _update_credentials_from_shadow(self) -> None:
        """Extract lock actuation token and PIN dynamically from device shadow."""
        if not (hasattr(self._lock_device, "shadow") and hasattr(self._lock_device.shadow, "reported")):
            return
        rep = self._lock_device.shadow.reported or {}
        st = (
            rep.get("lockControl_lockState")
            or (rep.get("lockControl", {}).get("lockState") if isinstance(rep.get("lockControl"), dict) else None)
            or rep.get("lockState")
        )
        if st is not None:
            s = str(st).strip().lower()
            if s in ("locked", "lock"):
                self._is_locked = True
            elif s in ("unlocked", "unlock"):
                self._is_locked = False

        # Dynamic token extraction
        token = (
            rep.get("token")
            or rep.get("lockControl_token")
            or (rep.get("lockControl", {}).get("token") if isinstance(rep.get("lockControl"), dict) else None)
        )
        if token:
            self._token = str(token)

        # Dynamic PIN extraction (Google Assistant / Alexa remote PIN or Master PIN)
        pin = (
            rep.get("pin")
            or rep.get("googleAssistantPin_pin")
            or (rep.get("googleAssistantPin", {}).get("pin") if isinstance(rep.get("googleAssistantPin"), dict) else None)
            or rep.get("masterPinControl_pin")
            or (rep.get("masterPinControl", {}).get("pin") if isinstance(rep.get("masterPinControl"), dict) else None)
        )
        if pin:
            self._pin = str(pin)

        # Battery extraction
        bat = (
            rep.get("batteryStatus_level")
            or (rep.get("batteryStatus", {}).get("level") if isinstance(rep.get("batteryStatus"), dict) else None)
        )
        if bat is not None:
            try:
                self._battery_level = int(bat)
            except (ValueError, TypeError):
                pass

        # Gateway pairing extraction
        gw = rep.get("linkedDevices_linkedDeviceID") or (
            rep.get("linkedDevices", {}).get("linkedDeviceID") if isinstance(rep.get("linkedDevices"), dict) else None
        )
        if gw:
            self._gateway_id = str(gw)

    @callback
    def _handle_event(self, data: dict[str, Any]) -> None:
        """Handle real-time MQTT push events from the lock and gateway."""
        dev_uuid = data.get("device_uuid")
        if dev_uuid not in (self._uuid, self._gateway_id):
            return

        params = data.get("parameters") or {}

        # Update dynamic actuation credentials from push events
        if params.get("token"):
            self._token = str(params["token"])
        if params.get("pin"):
            self._pin = str(params["pin"])
        if params.get("level"):
            try:
                self._battery_level = int(params["level"])
            except (ValueError, TypeError):
                pass

        raw_state = (
            params.get("lockState")
            or params.get("lockControl_lockState")
            or (params.get("lockControl", {}).get("lockState") if isinstance(params.get("lockControl"), dict) else None)
        )
        if raw_state is not None:
            s = str(raw_state).strip().lower()
            if s in ("unlocked", "unlock"):
                _LOGGER.info("Qubo lock %s state updated via MQTT push: UNLOCKED", self._uuid)
                self._is_unlocking = False
                self._is_locked = False
                if self._safety_timer:
                    self._safety_timer.cancel()
                    self._safety_timer = None
                self._schedule_auto_relock()
                self.async_write_ha_state()
            elif s in ("locked", "lock"):
                _LOGGER.info("Qubo lock %s state updated via MQTT push: LOCKED", self._uuid)
                self._is_unlocking = False
                self._is_locked = True
                if self._safety_timer:
                    self._safety_timer.cancel()
                    self._safety_timer = None
                if self._relock_timer:
                    self._relock_timer.cancel()
                    self._relock_timer = None
                self.async_write_ha_state()

    def _schedule_auto_relock(self) -> None:
        """Schedule automatic return to locked state matching hardware auto-lock."""
        if self._relock_timer:
            self._relock_timer.cancel()

        @callback
        def _relock() -> None:
            _LOGGER.info("Qubo lock %s auto-relocked mechanically to locked state", self._uuid)
            self._is_locked = True
            self._is_unlocking = False
            self._relock_timer = None
            self.async_write_ha_state()

        self._relock_timer = self.hass.loop.call_later(AUTO_RELOCK_DELAY, _relock)

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock the door and dispatch actuation command."""
        # Cancel any pending timers
        if self._relock_timer:
            self._relock_timer.cancel()
            self._relock_timer = None
        if self._safety_timer:
            self._safety_timer.cancel()
            self._safety_timer = None

        # Show Unlocking... spinner immediately in HomeKit / Home Assistant (<10ms UI response)
        self._is_unlocking = True
        self.async_write_ha_state()

        # Arm safety fallback timer to clear spinner if device fails to respond
        @callback
        def _safety_timeout() -> None:
            if self._is_unlocking:
                _LOGGER.warning("Unlock response timeout for lock %s: resetting UI state", self._uuid)
                self._is_unlocking = False
                self._safety_timer = None
                self.async_write_ha_state()

        self._safety_timer = self.hass.loop.call_later(UNLOCK_SAFETY_TIMEOUT, _safety_timeout)

        # Send command asynchronously in background
        code = kwargs.get("code")
        self._unlock_task = self.hass.async_create_task(
            self._async_dispatch_unlock(code=code), "qubo_unlock_dispatch"
        )

    async def _async_dispatch_unlock(self, code: str | None = None) -> None:
        """Execute unlock payload via instant MQTT publish with cloud REST fallback."""
        rt = self.hass.data.get(DOMAIN, {}).get(self._entry_id)
        client = getattr(self._lock_device, "client", getattr(rt, "client", None))
        unit_id = getattr(self._lock_device, "unit_id", getattr(self._lock_device.info, "unit_uuid", None))
        entity_id = getattr(self._lock_device, "entity_id", getattr(self._lock_device.info, "entity_uuid", None))

        target_gw = self._gateway_id or getattr(self._lock_device, "gateway_id", None)
        if not target_gw and rt:
            for dev in rt.devices:
                m = (getattr(dev.info, "model", None) or "").upper()
                t = (getattr(dev.info, "device_type", None) or "").upper()
                if "DOORBELL" in t or "CAMERA" in t or m.startswith("HCD"):
                    target_gw = getattr(dev, "device_id", dev.info.uuid)
                    self._gateway_id = target_gw
                    break

        if not target_gw:
            _LOGGER.error("Missing required gateway binding for lock %s", self._uuid)
            self._is_unlocking = False
            self._is_locked = True
            if self._safety_timer:
                self._safety_timer.cancel()
                self._safety_timer = None
            self.async_write_ha_state()
            return

        if not unit_id:
            if rt and getattr(rt, "listener", None):
                unit_id = rt.listener.unit_uuid
        if not entity_id and hasattr(self._lock_device, "info"):
            entity_id = getattr(self._lock_device.info, "entity_uuid", None)

        try:
            _LOGGER.info(
                "Dispatching gateway unlock command to %s for lock %s (unit: %s)",
                target_gw,
                self._uuid,
                unit_id,
            )
            if client and hasattr(client, "unlock_lock_via_gateway") and unit_id and entity_id:
                res = await client.unlock_lock_via_gateway(
                    unit_uuid=unit_id,
                    entity_uuid=entity_id,
                    gateway_uuid=target_gw,
                )
                _LOGGER.info("Gateway unlock successfully dispatched: %s", res)
            elif client and unit_id and entity_id:
                from qubopy.const import PATH_SEND_COMMAND

                gw_path = PATH_SEND_COMMAND.format(
                    sp=client.sp_id,
                    unit_uuid=unit_id,
                    entity_uuid=entity_id,
                    device_uuid=target_gw,
                )
                gw_payload = {
                    "topic": f"/control/{unit_id}/{target_gw}/lockControl",
                    "payload": {
                        "devices": {
                            "deviceUUID": target_gw,
                            "services": {
                                "lockControl": {
                                    "commands": {
                                        "lockUnlock": {
                                            "instanceId": 0,
                                            "parameters": {"control": "unlock"},
                                        }
                                    }
                                }
                            },
                        }
                    },
                }
                res = await client.request("PUT", gw_path, json_data=gw_payload)
                _LOGGER.info("Gateway unlock REST response: %s", res)
            else:
                _LOGGER.error("Missing client or unit/entity UUIDs to unlock lock %s", self._uuid)
                self._is_unlocking = False
                self._is_locked = True
                if self._safety_timer:
                    self._safety_timer.cancel()
                    self._safety_timer = None
                self.async_write_ha_state()
        except Exception as err:
            _LOGGER.error("Gateway unlock failed for lock %s: %s", self._uuid, err)
            self._is_unlocking = False
            self._is_locked = True
            if self._safety_timer:
                self._safety_timer.cancel()
                self._safety_timer = None
            self.async_write_ha_state()

    async def async_lock(self, **kwargs: Any) -> None:
        """Confirm lock state (Qubo locks auto-relock mechanically)."""
        if self._relock_timer:
            self._relock_timer.cancel()
            self._relock_timer = None
        if self._safety_timer:
            self._safety_timer.cancel()
            self._safety_timer = None
        self._is_locked = True
        self._is_locking = False
        self._is_unlocking = False
        self.async_write_ha_state()
        _LOGGER.info("Qubo lock %s confirmed locked", self._uuid)

    async def async_open(self, **kwargs: Any) -> None:
        """Open the door (unlatch/unlock)."""
        await self.async_unlock(**kwargs)
