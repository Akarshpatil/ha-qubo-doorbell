"""Binary sensors for Qubo devices: Doorbell Ring, Motion, and Lock Door Contact."""
from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later
from qubopy.devices.lock import QuboLock

from .const import DOMAIN, RING_KEYWORDS, RING_ON_SECONDS, SIGNAL_EVENT
from .entity import QuboEntity, is_camera_like


def is_doorbell_like(device: Any) -> bool:
    """Return True if device is a video doorbell."""
    model = (getattr(device.info, "model", None) or "").upper()
    dtype = (getattr(device.info, "device_type", None) or "").upper()
    return model.startswith("HCD") or "DOORBELL" in dtype or "VDB" in dtype


def is_lock_device(device: Any) -> bool:
    """Return True if device is a smart lock."""
    if isinstance(device, QuboLock):
        return True
    m = (getattr(device.info, "model", None) or "").upper()
    t = (getattr(device.info, "device_type", None) or "").upper()
    return "LOCK" in t or any(m.startswith(p) for p in ("HLM", "HLW", "HLR"))


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    rt = hass.data[DOMAIN][entry.entry_id]
    ents: list[BinarySensorEntity] = []
    for d in rt.devices:
        if is_doorbell_like(d):
            ents.append(QuboRingSensor(entry.entry_id, d))
        if is_camera_like(d):
            ents.append(QuboMotionSensor(entry.entry_id, d))
        if is_lock_device(d):
            ents.append(QuboDoorContactSensor(entry.entry_id, d))
    async_add_entities(ents)


def _text(data: dict[str, Any]) -> str:
    return " ".join(str(data.get(k) or "") for k in ("service", "event_name", "parameters")).lower()


class _PulseSensor(QuboEntity, BinarySensorEntity):
    _match: tuple[str, ...] = ()

    def __init__(self, entry_id: str, device: Any) -> None:
        super().__init__(entry_id, device)
        self._attr_is_on = False
        self._cancel = None
        self._attr_extra_state_attributes = {}

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(self.hass, f"{SIGNAL_EVENT}_{self._entry_id}", self._handle)
        )

    def _matches(self, data: dict[str, Any]) -> bool:
        return any(k in _text(data) for k in self._match)

    @callback
    def _handle(self, data: dict[str, Any]) -> None:
        if data["device_uuid"] != self._uuid or not self._matches(data):
            return
        self._attr_is_on = True
        self._attr_extra_state_attributes = {"service": data.get("service"), "event_name": data.get("event_name")}
        self.async_write_ha_state()
        if self._cancel:
            self._cancel()
        self._cancel = async_call_later(self.hass, RING_ON_SECONDS, self._off)

    @callback
    def _off(self, _now: Any) -> None:
        self._cancel = None
        self._attr_is_on = False
        self.async_write_ha_state()


class QuboRingSensor(_PulseSensor):
    _attr_name = "Ring"
    _attr_icon = "mdi:doorbell"
    _attr_device_class = BinarySensorDeviceClass.OCCUPANCY
    _match = RING_KEYWORDS

    def __init__(self, entry_id: str, device: Any) -> None:
        super().__init__(entry_id, device)
        self._attr_unique_id = f"{self._uuid}_ring"

    def _matches(self, data: dict[str, Any]) -> bool:
        # Verified on HCD04C: a button press sends service=doorbell, event=doorbellPressed
        return (
            (data.get("service") == "doorbell" and data.get("event_name") == "doorbellPressed")
            or any(k in _text(data) for k in ("doorbellpressed", "ring", "bell", "press"))
        )


class QuboMotionSensor(_PulseSensor):
    _attr_name = "Motion"
    _attr_device_class = BinarySensorDeviceClass.MOTION
    _match = ("motion", "person", "ooi")

    def __init__(self, entry_id: str, device: Any) -> None:
        super().__init__(entry_id, device)
        self._attr_unique_id = f"{self._uuid}_motion"


class QuboDoorContactSensor(QuboEntity, BinarySensorEntity):
    """Door open/close contact sensor for smart locks."""

    _attr_name = "Door"
    _attr_device_class = BinarySensorDeviceClass.DOOR

    def __init__(self, entry_id: str, device: Any) -> None:
        super().__init__(entry_id, device)
        self._attr_unique_id = f"{self._uuid}_door_contact"
        self._attr_is_on = None

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(self.hass, f"{SIGNAL_EVENT}_{self._entry_id}", self._handle)
        )

    @callback
    def _handle(self, data: dict[str, Any]) -> None:
        if data.get("device_uuid") != self._uuid:
            return
        params = data.get("parameters") or {}
        raw = params.get("isOpen") or params.get("doorState") or params.get("open")
        if raw is not None:
            if isinstance(raw, bool):
                self._attr_is_on = raw
            elif isinstance(raw, str):
                self._attr_is_on = raw.lower() in ("open", "opened", "true", "1")
            self.async_write_ha_state()
