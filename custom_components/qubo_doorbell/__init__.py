"""Qubo Doorbell integration (unofficial, built on qubopy)."""
from __future__ import annotations

from collections import deque
from datetime import timedelta
import json
import logging
import os
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD, Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_connect, async_dispatcher_send
from homeassistant.helpers.event import async_track_time_interval
from qubopy import QuboAuth, QuboAuthError, QuboClient, QuboConnectionError, QuboMqttListener

from .const import CONF_DEVICE_ID, DOMAIN, EVENT_QUBO, MAX_EVENT_LOG, SIGNAL_EVENT
from .entity import is_camera_like
from .motion import MotionCapture

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR, Platform.IMAGE, Platform.LOCK, Platform.CAMERA]


def _patch_homekit_camera_lifecycle() -> None:
    """Ensure HomeKit camera accessories advertise as Video Doorbell, use zero-latency settings, and release streams on close."""
    try:
        from homeassistant.components.homekit import type_cameras
        from homeassistant.components.homekit.const import (
            CONF_AUDIO_CODEC,
            CONF_LINKED_DOORBELL_SENSOR,
            CONF_SUPPORT_AUDIO,
            CONF_VIDEO_CODEC,
        )
        from pyhap.const import CATEGORY_VIDEO_DOOR_BELL

        if getattr(type_cameras.Camera, "_qubo_doorbell_patched", False):
            return

        _orig_init = type_cameras.Camera.__init__
        _orig_stop_stream = type_cameras.Camera.stop_stream
        _orig_get_stream_source = type_cameras.Camera._async_get_stream_source

        def _doorbell_aware_init(self: Any, *args: Any, **kwargs: Any) -> None:
            _orig_init(self, *args, **kwargs)
            is_qubo = "qubo" in getattr(self, "platform", "").lower() or "qubo" in getattr(self, "entity_id", "").lower()
            if self.config.get(CONF_LINKED_DOORBELL_SENSOR) or is_qubo:
                if self.config.get(CONF_LINKED_DOORBELL_SENSOR):
                    self.category = CATEGORY_VIDEO_DOOR_BELL
                    _LOGGER.info(
                        "HomeKit camera accessory '%s' configured with linked doorbell: Set category to CATEGORY_VIDEO_DOOR_BELL (18)",
                        getattr(self, "display_name", self.entity_id),
                    )
                # Optimize for sub-second zero-latency playback: pass native H.264 without CPU transcoding
                if self.config.get(CONF_VIDEO_CODEC) != "copy":
                    self.config[CONF_VIDEO_CODEC] = "copy"
                    _LOGGER.debug("Enforced video_codec='copy' for %s for zero transcoding latency", self.entity_id)
                # Enable live camera listening audio
                self.config[CONF_SUPPORT_AUDIO] = True

        async def _qubo_get_stream_source(self: Any) -> str | None:
            src = await _orig_get_stream_source(self)
            if not src:
                return src
            is_qubo = "qubo" in getattr(self, "platform", "").lower() or "qubo" in getattr(self, "entity_id", "").lower() or "qubo" in src.lower()
            if is_qubo:
                # Extract raw RTSP URL if input options are already attached
                target_url = src.split("-i ")[-1].strip() if "-i " in src else src
                # Enforce ultra-fast stream probing and zero-buffering low-delay transport
                src = (
                    f"-probesize 32768 -analyzeduration 100000 -fpsprobesize 0 "
                    f"-fflags nobuffer+flush_packets -flags low_delay -rtsp_transport tcp -i {target_url}"
                )
            return src

        async def _doorbell_aware_stop_stream(self: Any, session_info: dict[str, Any]) -> None:
            try:
                await _orig_stop_stream(self, session_info)
            finally:
                session_id = session_info.get("id")
                try:
                    cam_comp = self.hass.data.get("camera")
                    entity = cam_comp.get_entity(self.entity_id) if cam_comp else None
                    if entity and hasattr(entity, "async_release_stream"):
                        await entity.async_release_stream(f"homekit_{session_id}")
                except Exception as err:
                    _LOGGER.debug("Error releasing stream from HomeKit stop_stream: %s", err)

        type_cameras.Camera.__init__ = _doorbell_aware_init
        type_cameras.Camera._async_get_stream_source = _qubo_get_stream_source
        type_cameras.Camera.stop_stream = _doorbell_aware_stop_stream
        type_cameras.Camera._qubo_doorbell_patched = True
        _LOGGER.debug("Applied HomeKit CATEGORY_VIDEO_DOOR_BELL, low-latency & stream teardown patches")
    except Exception as err:
        _LOGGER.debug("Could not apply HomeKit doorbell patches: %s", err)


_patch_homekit_camera_lifecycle()


class QuboRuntime:
    """Holds the live Qubo session for one config entry."""

    def __init__(self, auth: Any, client: Any, devices: list[Any]) -> None:
        self.auth = auth
        self.client = client
        self.devices = devices
        self.listener: Any = None
        self.events: deque[dict[str, Any]] = deque(maxlen=MAX_EVENT_LOG)
        self.connected = False


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Log in, discover devices and start the real-time event listener."""
    _patch_homekit_camera_lifecycle()
    session = async_get_clientsession(hass)
    auth = QuboAuth(device_id=entry.data[CONF_DEVICE_ID], session=session)
    cache_file = hass.config.path(".storage", "qubo_p2p_cache.json")
    authenticated = False

    def _read_cache_sync() -> dict[str, Any]:
        if os.path.isfile(cache_file):
            try:
                with open(cache_file, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _write_cache_sync(data_to_write: dict[str, Any]) -> None:
        try:
            with open(cache_file, "w") as f:
                json.dump(data_to_write, f, indent=2)
        except Exception as err:
            _LOGGER.debug("Could not write cache file: %s", err)

    # 1. Attempt fast session restoration from shared persistent cache
    cdata = await hass.async_add_executor_job(_read_cache_sync)
    if cdata:
        try:
            c_tok = cdata.get("access_token")
            c_ref = cdata.get("refresh_token")
            c_uuid = cdata.get("user_uuid", "")
            c_exp = cdata.get("expires_at", 0)
            if c_tok and (c_exp - time.time()) > 300:
                auth.set_tokens(access_token=c_tok, refresh_token=c_ref, user_uuid=c_uuid, expires_at=c_exp)
                authenticated = True
                _LOGGER.info("Restored Qubo cloud session from shared cache (valid for %ds)", int(c_exp - time.time()))
            elif c_ref and c_uuid:
                try:
                    auth.set_tokens(access_token=c_tok or "", refresh_token=c_ref, user_uuid=c_uuid, expires_at=0)
                    new_tok = await auth.refresh_session()
                    cdata.update({
                        "access_token": new_tok.access_token,
                        "refresh_token": new_tok.refresh_token,
                        "user_uuid": new_tok.user_uuid,
                        "expires_at": new_tok.expires_at,
                    })
                    await hass.async_add_executor_job(_write_cache_sync, cdata)
                    authenticated = True
                    _LOGGER.info("Refreshed Qubo cloud session token from shared cache")
                except Exception as err:
                    _LOGGER.debug("Could not refresh token from shared cache: %s", err)
        except Exception as err:
            _LOGGER.debug("Error reading shared token cache: %s", err)

    if not authenticated:
        try:
            tok = await auth.login_with_email(
                entry.data[CONF_EMAIL], entry.data[CONF_PASSWORD], device_attribute="Home Assistant"
            )
            cdata = await hass.async_add_executor_job(_read_cache_sync)
            cdata.update({
                "access_token": tok.access_token,
                "refresh_token": tok.refresh_token,
                "user_uuid": tok.user_uuid,
                "expires_at": tok.expires_at,
                "email": entry.data[CONF_EMAIL],
            })
            await hass.async_add_executor_job(_write_cache_sync, cdata)
        except QuboAuthError as err:
            raise ConfigEntryAuthFailed(f"Qubo login failed: {err}") from err
        except (QuboConnectionError, OSError) as err:
            raise ConfigEntryNotReady(f"Cannot reach Qubo cloud: {err}") from err

    try:
        client = QuboClient(auth, session=session)
        devices = [
            d for d in await client.get_all_devices()
            if (d.info.model or "").lower() != "mobile" and not (d.info.name or "").startswith("MobileApp_")
        ]
    except QuboAuthError as err:
        raise ConfigEntryAuthFailed(f"Qubo login failed: {err}") from err
    except (QuboConnectionError, OSError) as err:
        raise ConfigEntryNotReady(f"Cannot reach Qubo cloud: {err}") from err

    rt = QuboRuntime(auth, client, devices)
    names = {d.info.uuid: d.info.name for d in devices}
    unit_uuids = sorted({d.info.unit_uuid for d in devices if d.info.unit_uuid})

    @callback
    def _on_event(ev: Any) -> None:
        data = {
            "device_uuid": ev.device_uuid,
            "device_name": names.get(ev.device_uuid, ev.device_uuid),
            "service": ev.service,
            "event_name": ev.event_name,
            "operation_state": ev.operation_state,
            "parameters": ev.parameters,
            "qubo_timestamp": ev.timestamp,
            "received": time.time(),
        }
        rt.events.appendleft(data)
        _LOGGER.info("Qubo event from %s: %s / %s %s", data["device_name"], ev.service, ev.event_name, ev.parameters)
        hass.bus.async_fire(EVENT_QUBO, data)
        async_dispatcher_send(hass, f"{SIGNAL_EVENT}_{entry.entry_id}", data)

    async def _on_auth_failure() -> None:
        _LOGGER.warning("Qubo session expired; re-logging in")
        try:
            tok = await auth.login_with_email(
                entry.data[CONF_EMAIL], entry.data[CONF_PASSWORD], device_attribute="Home Assistant"
            )
            cdata = await hass.async_add_executor_job(_read_cache_sync)
            cdata.update({
                "access_token": tok.access_token,
                "refresh_token": tok.refresh_token,
                "user_uuid": tok.user_uuid,
                "expires_at": tok.expires_at,
                "email": entry.data[CONF_EMAIL],
            })
            await hass.async_add_executor_job(_write_cache_sync, cdata)
            _LOGGER.info("Qubo re-login succeeded")
        except QuboAuthError as err:
            _LOGGER.error("Qubo credentials rejected: %s", err)
            entry.async_start_reauth(hass)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Qubo re-login failed due to network or server outage (will retry): %s", err)

    listener = QuboMqttListener(
        client, unit_uuids=unit_uuids or None, loop=hass.loop, on_auth_failure=_on_auth_failure
    )
    listener.add_event_listener(_on_event)
    try:
        await listener.start()
        rt.connected = True
    except Exception as err:  # noqa: BLE001
        raise ConfigEntryNotReady(f"Qubo real-time connection failed: {err}") from err
    rt.listener = listener

    async def _keep_token_fresh(_now: Any) -> None:
        try:
            await auth.get_valid_access_token()
        except QuboAuthError as err:
            _LOGGER.warning("Qubo token refresh auth error: %s", err)
            await _on_auth_failure()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Qubo token refresh deferred due to temporary connection error: %s", err)

    entry.async_on_unload(async_track_time_interval(hass, _keep_token_fresh, timedelta(minutes=20)))

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = rt
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    for d in devices:
        if is_camera_like(d) and hasattr(d, "start_stream"):
            cap = MotionCapture(hass, entry, d)
            entry.async_on_unload(async_dispatcher_connect(hass, f"{SIGNAL_EVENT}_{entry.entry_id}", cap.handle))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _LOGGER.info(
        "Qubo connected: %d device(s): %s",
        len(devices),
        ", ".join(f"{d.info.name} ({d.info.model})" for d in devices),
    )
    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_remove_config_entry_device(hass: HomeAssistant, entry: ConfigEntry, device: Any) -> bool:
    """Allow removing stale devices from the UI."""
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Stop the listener and unload platforms."""
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    rt: QuboRuntime | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if rt and rt.listener:
        try:
            await rt.listener.stop()
        except Exception:  # noqa: BLE001
            pass
    return ok
