"""Live camera platform for Qubo Video Doorbell and cameras with strict single-stream concurrency and instant snapshot preloading."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from typing import Any

import aiohttp

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.stream import Stream
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later
from qubopy.devices.camera import QuboCamera

from .const import DOMAIN, SIGNAL_EVENT, SNAPSHOT_KEYS
from .entity import QuboEntity, is_camera_like

_LOGGER = logging.getLogger(__name__)
WARMUP_SECONDS = 0.5
MAX_IMAGE_BYTES = 10 * 1024 * 1024


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Qubo camera entities."""
    rt = hass.data[DOMAIN][entry.entry_id]
    cams = [
        QuboLiveCamera(hass, entry.entry_id, d)
        for d in rt.devices
        if is_camera_like(d) and hasattr(d, "start_stream")
    ]
    if cams:
        _LOGGER.info("Setting up %d Qubo live camera(s)", len(cams))
        async_add_entities(cams)


class QuboLiveCamera(QuboEntity, Camera):
    """Live streaming camera entity for Qubo video doorbells with strict single-stream concurrency."""

    _attr_has_entity_name = True
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(self, hass: HomeAssistant, entry_id: str, device: QuboCamera) -> None:
        QuboEntity.__init__(self, entry_id, device)
        Camera.__init__(self)
        self.hass = hass
        self._cam_device: QuboCamera = device
        self._attr_unique_id = f"{self._uuid}_live_feed"
        self._attr_name = "Live Feed"
        self._snapshot_path = hass.config.path(DOMAIN, f"{self._uuid}_snapshot.jpg")
        self._legacy_snapshot_path = hass.config.path(DOMAIN, f"{self._uuid}_last_visitor.jpg")
        self._last_image: bytes | None = None
        self._last_image_mtime: float = 0.0
        self._last_known_event_ts: int = 0
        self._stream_lock = asyncio.Lock()
        self._last_cloud_fetch = 0.0

        # Concurrency & session management
        self._active_stream_url: str | None = None
        self._active_stream_session: str | None = None
        self._active_user_app_id: str | None = None
        self._active_consumers: set[str] = set()
        self._watchdog_unsub: Any = None

    async def async_added_to_hass(self) -> None:
        """Prime snapshot on boot and listen for doorbell/motion events."""
        await super().async_added_to_hass()

        disk_snap = await self.hass.async_add_executor_job(self._read_disk_sync)
        if disk_snap:
            self._last_image, self._last_image_mtime = disk_snap

        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{SIGNAL_EVENT}_{self._entry_id}", self._handle_event
            )
        )

        self.hass.async_create_background_task(
            self._async_fetch_cloud_snapshot(), "qubo_preload_snapshot"
        )

    def _read_disk_sync(self) -> tuple[bytes, float] | None:
        """Read the newest valid JPEG from disk cache synchronously."""
        candidates: list[tuple[float, str]] = []
        for path in (self._snapshot_path, self._legacy_snapshot_path):
            if os.path.isfile(path):
                try:
                    mtime = os.path.getmtime(path)
                    candidates.append((mtime, path))
                except OSError:
                    pass
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        for mtime, path in candidates:
            try:
                with open(path, "rb") as f:
                    data = f.read()
                if data and data[:3] == b"\xff\xd8\xff":
                    return data, mtime
            except Exception:
                pass
        return None

    def _get_disk_mtime_sync(self) -> float:
        """Get the newest mtime among snapshot files on disk."""
        latest = 0.0
        for path in (self._snapshot_path, self._legacy_snapshot_path):
            if os.path.isfile(path):
                try:
                    mtime = os.path.getmtime(path)
                    if mtime > latest:
                        latest = mtime
                except OSError:
                    pass
        return latest

    @callback
    def _handle_event(self, data: dict[str, Any]) -> None:
        """Handle incoming real-time motion/doorbell events to refresh snapshot."""
        if data.get("device_uuid") != self._uuid:
            return
        svc = data.get("service")
        ev = data.get("event_name")
        params = data.get("parameters") or {}

        # 1. Direct photo URL in event payload (motion, person, or ring)
        url = next(
            (
                params[k]
                for k in SNAPSHOT_KEYS
                if isinstance(params.get(k), str) and params[k].startswith("http")
            ),
            None,
        )
        if url:
            _LOGGER.info("Qubo real-time event carried snapshot URL: %s", ev)
            self.hass.async_create_background_task(
                self._async_download_url(url), "qubo_event_url_snapshot"
            )
            return

        # 2. Doorbell press, ring, call, or detection alert without direct URL
        if (
            svc in ("doorbell", "call", "recordingControl")
            or ev in ("doorbellPressed", "motionDetected", "personDetected", "incomingCall")
        ):
            self.hass.loop.call_later(
                2.0,
                lambda: self.hass.async_create_background_task(
                    self._async_fetch_cloud_snapshot(), "qubo_event_snapshot"
                ),
            )

    async def _async_download_url(self, url: str) -> bytes | None:
        """Download snapshot from pre-signed URL and update cache."""
        session = async_get_clientsession(self.hass)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    if content and content[:3] == b"\xff\xd8\xff" and len(content) <= MAX_IMAGE_BYTES:
                        self._last_image = content
                        self._last_image_mtime = time.time()
                        def _save():
                            os.makedirs(os.path.dirname(self._snapshot_path), exist_ok=True)
                            with open(self._snapshot_path, "wb") as f:
                                f.write(content)
                            try:
                                shutil.copyfile(self._snapshot_path, self._legacy_snapshot_path)
                            except Exception:
                                pass
                            try:
                                www_path = self.hass.config.path("www", "qubo_visitor_latest.jpg")
                                os.makedirs(os.path.dirname(www_path), exist_ok=True)
                                shutil.copyfile(self._snapshot_path, www_path)
                            except Exception:
                                pass
                        await self.hass.async_add_executor_job(_save)
                        self.async_write_ha_state()
                        _LOGGER.info("Updated Qubo snapshot for %s (%d bytes)", self.name, len(content))
                        return content
        except Exception as err:
            _LOGGER.debug("Could not download event snapshot URL for %s: %s", self.name, err)
        return None

    async def _async_fetch_cloud_snapshot(self) -> bytes | None:
        """Fetch the latest event snapshot from Qubo cloud VMS and cache in RAM and disk."""
        now = time.monotonic()
        if now - self._last_cloud_fetch < 5.0:
            return self._last_image
        self._last_cloud_fetch = now

        try:
            events = await self._cam_device.get_vms_doorbell_events(record_number=3)
            for ev in events:
                created_ts = ev.get("createdTimestamp") or 0
                snap = ev.get("snapshot") or {}
                url = snap.get("downloadURL")
                if not url:
                    continue
                # If we already have this event cached and in RAM, skip re-downloading
                if self._last_known_event_ts and created_ts <= self._last_known_event_ts and self._last_image:
                    return self._last_image

                session = async_get_clientsession(self.hass)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        if content and content[:3] == b"\xff\xd8\xff":
                            self._last_image = content
                            self._last_image_mtime = time.time()
                            self._last_known_event_ts = created_ts
                            _LOGGER.info(
                                "Downloaded fresh Qubo snapshot (%d bytes, event_ts=%s) for %s",
                                len(content),
                                created_ts,
                                self.name,
                            )
                            def _save():
                                os.makedirs(os.path.dirname(self._snapshot_path), exist_ok=True)
                                with open(self._snapshot_path, "wb") as f:
                                    f.write(content)
                                try:
                                    shutil.copyfile(self._snapshot_path, self._legacy_snapshot_path)
                                except Exception:
                                    pass
                                try:
                                    www_path = self.hass.config.path("www", "qubo_visitor_latest.jpg")
                                    os.makedirs(os.path.dirname(www_path), exist_ok=True)
                                    shutil.copyfile(self._snapshot_path, www_path)
                                except Exception:
                                    pass
                            await self.hass.async_add_executor_job(_save)
                            self.async_write_ha_state()
                            return content
        except Exception as err:
            _LOGGER.debug("Could not fetch cloud snapshot for %s: %s", self.name, err)
        return self._last_image

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return the preloaded snapshot instantly for Apple Home and UI preview without starting a video stream."""
        if self.stream and getattr(self.stream, "available", False):
            try:
                frame = await self.stream.async_get_image(width=width, height=height)
                if frame:
                    self._last_image = frame
                    return frame
            except Exception as err:
                _LOGGER.debug("Error grabbing frame from stream: %s", err)

        disk_mtime = await self.hass.async_add_executor_job(self._get_disk_mtime_sync)
        if disk_mtime > self._last_image_mtime or not self._last_image:
            fresh = await self.hass.async_add_executor_job(self._read_disk_sync)
            if fresh:
                self._last_image, self._last_image_mtime = fresh
                return self._last_image

        if self._last_image:
            return self._last_image

        self.hass.async_create_background_task(
            self._async_fetch_cloud_snapshot(), "qubo_fetch_snapshot_fallback"
        )
        return self._last_image

    async def async_create_stream(self) -> Stream | None:
        """Create a fresh Stream, ensuring dead or stale stream workers are reset and hooked."""
        if self.stream is not None:
            if not getattr(self.stream, "available", False):
                try:
                    await self.stream.stop()
                except Exception:
                    pass
                self.stream = None
        stream = await super().async_create_stream()
        if stream:
            self._active_consumers.add("ha_dashboard")
            orig_stop = stream.stop

            async def _hooked_stop(*args: Any, **kwargs: Any) -> None:
                try:
                    await orig_stop(*args, **kwargs)
                finally:
                    await self.async_release_stream("ha_dashboard")

            stream.stop = _hooked_stop
        return stream

    def _reset_watchdog(self) -> None:
        """Reset auto-teardown watchdog timer (safety net for orphaned sessions)."""
        if self._watchdog_unsub:
            self._watchdog_unsub()
            self._watchdog_unsub = None

        @callback
        def _on_timeout(_now: Any) -> None:
            _LOGGER.info("Watchdog timer expired for Qubo live stream; auto-releasing hardware stream")
            self.hass.async_create_background_task(
                self.async_release_stream(), "qubo_watchdog_release"
            )

        # 3 minutes maximum streaming session before auto-release
        self._watchdog_unsub = async_call_later(self.hass, 180, _on_timeout)

    async def async_release_stream(self, consumer_id: str | None = None) -> None:
        """Release a stream consumer and tear down Qubo hardware stream if no active viewers remain."""
        async with self._stream_lock:
            if consumer_id:
                self._active_consumers.discard(consumer_id)
                if len(self._active_consumers) == 1 and "default" in self._active_consumers:
                    self._active_consumers.clear()
                _LOGGER.info("Released stream consumer %s (active remaining: %s)", consumer_id, self._active_consumers)
            else:
                self._active_consumers.clear()

            if not self._active_consumers and (self._active_stream_url or self._active_stream_session):
                if self._watchdog_unsub:
                    self._watchdog_unsub()
                    self._watchdog_unsub = None

                session_id = self._active_stream_session
                user_app_id = self._active_user_app_id
                self._active_stream_url = None
                self._active_stream_session = None
                self._active_user_app_id = None

                _LOGGER.info("No active viewers remaining. Tearing down Qubo cloud/hardware stream session.")
                try:
                    # 1. v2 teardown via qubopy device
                    if session_id:
                        await self._cam_device.stop_stream(
                            app_session_id=session_id, quality="high", user_app_id=user_app_id
                        )
                    # 2. v1 direct DELETE fallback (ensures hardware release regardless of appSessionId)
                    client = getattr(self._cam_device, "client", None)
                    if client and hasattr(client, "request"):
                        app_id = user_app_id or client.auth.device_id
                        v1_path = (
                            f"/stream-manager/api/v1/sp/{client.sp_id}/stream/unit/"
                            f"{self._cam_device.unit_id}/device/{self._cam_device.device_id}"
                        )
                        await client.request("DELETE", v1_path, params={"type": "high", "userAppID": app_id})
                    _LOGGER.info("Successfully released Qubo stream session. Blue ring LED will extinguish.")
                except Exception as err:
                    _LOGGER.debug("Stream teardown completed with notice: %s", err)

    async def stream_source(self) -> str | None:
        """Return live RTSP stream source via local P2P gateway or cloud fallback."""
        # 1. Primary: Local P2P via MediaMTX gateway
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", 8554), timeout=1.0
            )
            writer.close()
            await writer.wait_closed()
            _LOGGER.info(
                "Routing %s live stream through local P2P gateway (rtsp://127.0.0.1:8554/qubo_local)",
                self.name,
            )
            return "rtsp://127.0.0.1:8554/qubo_local"
        except Exception as err:
            _LOGGER.debug("Local MediaMTX gateway not reachable at 127.0.0.1:8554: %s", err)

        # 2. Secondary fallback: Cloud RTSPS stream from Qubo Cloud
        async with self._stream_lock:
            # 1. If an active stream is already running, reuse it!
            if self._active_stream_url:
                _LOGGER.info(
                    "Reusing existing live RTSPS stream for %s (consumers: %s)",
                    self.name,
                    self._active_consumers,
                )
                self._active_consumers.add("default")
                self._reset_watchdog()
                return self._active_stream_url

            # 2. Otherwise request fresh RTSPS stream from Qubo Cloud
            try:
                user_app_id = f"ha_{self._uuid[:8]}"
                _LOGGER.info("Requesting fresh cloud RTSPS stream for %s (userAppID=%s)", self.name, user_app_id)
                url = await self._cam_device.start_stream(quality="high", user_app_id=user_app_id)
                if url:
                    self._active_stream_url = str(url)
                    self._active_stream_session = getattr(url, "app_session_id", None)
                    self._active_user_app_id = user_app_id
                    self._active_consumers.add("default")
                    self._reset_watchdog()
                    if WARMUP_SECONDS > 0:
                        await asyncio.sleep(WARMUP_SECONDS)
                    return self._active_stream_url
            except Exception as err:
                _LOGGER.warning(
                    "Cloud stream for %s unavailable (%s). Enforcing single hardware stream exclusivity.",
                    self.name,
                    err,
                )
                self._active_stream_url = None
                self._active_stream_session = None
                self._active_user_app_id = None
                return None

    async def async_will_remove_from_hass(self) -> None:
        """Tear down stream session upon entity removal."""
        await self.async_release_stream()
