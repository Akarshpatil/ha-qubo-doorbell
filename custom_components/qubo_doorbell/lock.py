"""Smart lock platform for Qubo smart door locks (HLM/HLW/HLR) with responsive actuation and auto-relock."""
from __future__ import annotations

import asyncio
import logging
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
    """Representation of a Qubo auto-relocking smart door lock with responsive actuation."""

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
        self._relock_timer: asyncio.TimerHandle | None = None
        self._unlock_task: asyncio.Task[None] | None = None

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
            "battery_level": getattr(self._lock_device, "battery_percent", None),
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
        """Prefetch lock status from shadow."""
        if hasattr(self._lock_device, "refresh"):
            try:
                await self._lock_device.refresh()
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
                        await self._lock_device.fetch_live_battery()
                    except Exception:
                        pass
                self.async_write_ha_state()
            except Exception as err:
                _LOGGER.debug("Initial lock shadow refresh deferred for %s: %s", self._uuid, err)

    @callback
    def _handle_event(self, data: dict[str, Any]) -> None:
        """Handle real-time MQTT push events."""
        dev_uuid = data.get("device_uuid")
        if dev_uuid not in (self._uuid, self._gateway_id):
            return

        params = data.get("parameters") or {}
        raw_state = (
            params.get("lockState")
            or params.get("lockControl_lockState")
            or (params.get("lockControl", {}).get("lockState") if isinstance(params.get("lockControl"), dict) else None)
        )
        if raw_state is not None:
            s = str(raw_state).strip().lower()
            if s in ("unlocked", "unlock"):
                self._is_unlocking = False
                self._is_locked = False
                self._schedule_auto_relock()
            elif s in ("locked", "lock"):
                self._is_unlocking = False
                self._is_locked = True
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
        # Cancel any pending relock timer
        if self._relock_timer:
            self._relock_timer.cancel()
            self._relock_timer = None

        # Show Unlocking... spinner immediately in HomeKit / Home Assistant
        self._is_unlocking = True
        self.async_write_ha_state()

        # Send command asynchronously in background
        self._unlock_task = self.hass.async_create_task(
            self._async_dispatch_unlock(), "qubo_unlock_dispatch"
        )

    async def _async_dispatch_unlock(self) -> None:
        """Execute unlock payload via paired in-home gateway (VDB)."""
        target_gw = self._gateway_id or getattr(self._lock_device, "gateway_id", None)
        if not target_gw:
            rt = self.hass.data.get(DOMAIN, {}).get(self._entry_id)
            if rt:
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
            self.async_write_ha_state()
            return

        try:
            if hasattr(self._lock_device, "unlock"):
                res = await self._lock_device.unlock(gateway_uuid=target_gw)
                _LOGGER.info("Unlock dispatched via gateway %s for lock %s: %s", target_gw, self._uuid, res)
                self._is_unlocking = False
                self._is_locked = False
                self.async_write_ha_state()
                self._schedule_auto_relock()
            else:
                _LOGGER.error("Lock device %s missing unlock method", self._uuid)
                self._is_unlocking = False
                self._is_locked = True
                self.async_write_ha_state()
        except Exception as err:
            _LOGGER.error("Unlock via gateway failed for lock %s: %s", self._uuid, err)
            self._is_unlocking = False
            self._is_locked = True
            self.async_write_ha_state()

    async def async_lock(self, **kwargs: Any) -> None:
        """Confirm lock state (Qubo locks auto-relock mechanically)."""
        if self._relock_timer:
            self._relock_timer.cancel()
            self._relock_timer = None
        self._is_locked = True
        self._is_locking = False
        self._is_unlocking = False
        self.async_write_ha_state()
        _LOGGER.info("Qubo lock %s confirmed locked", self._uuid)

    async def async_open(self, **kwargs: Any) -> None:
        """Open the door (unlatch/unlock)."""
        await self.async_unlock(**kwargs)
