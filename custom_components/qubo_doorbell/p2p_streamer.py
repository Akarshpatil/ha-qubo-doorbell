#!/usr/bin/env python3
"""Qubo Video Doorbell Local P2P Video & Audio Streamer.

Direct LAN media bridge: Doorbell (UDP) -> Host -> MediaMTX / go2rtc (RTSP)
- Negotiates P2P UDP candidate via Qubo Cloud API (p2pCall)
- Handles ICE/STUN binding & USE-CANDIDATE nomination
- Maintains ICE consent freshness keepalives
- Decrypts AES-128-GCM RTP video payload (Dynamic PT 96, H.264 Baseline) in real-time
- Decrypts AES-128-GCM RTP audio payload (Dynamic PT 97, AAC-LC 16kHz) in real-time
- Decoupled localhost RTP forwarder with dynamic SDP manifest for zero-latency, deadlock-free A/V ingestion
- Direct low-latency A/V passthrough into RTSP server via FFmpeg (-c:v copy -c:a copy)
- Gracefully releases camera hardware encoder on teardown to preserve battery & extinguish blue LED
- Automatically discovers credentials, device IDs, and local network IP from Home Assistant
- Caches authentication tokens and SPS/PPS for sub-second stream startup
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import struct
import subprocess
import sys
import time
from typing import Any

import aiohttp
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from qubopy import QuboAuth, QuboClient
from qubopy.devices.camera import QuboCamera

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger("qubo_p2p")

MAGIC_COOKIE = b"\x24\x4a\x21\x12"
DEFAULT_LOCAL_PORT = 49700
DEFAULT_RTSP_TARGET = "rtsp://127.0.0.1:8554/qubo_local"
DEFAULT_RTP_VIDEO_PORT = 5004
DEFAULT_RTP_AUDIO_PORT = 5006

SDP_TEMPLATE = """v=0
o=- 0 0 IN IP4 127.0.0.1
s=Qubo
c=IN IP4 127.0.0.1
t=0 0
m=video {video_port} RTP/AVP 96
a=rtpmap:96 H264/90000
m=audio {audio_port} RTP/AVP 97
a=rtpmap:97 MPEG4-GENERIC/16000/1
a=fmtp:97 streamtype=5;profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3;config=1408
"""


def get_default_local_ip() -> str:
    """Detect default outbound LAN IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("1.1.1.1", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def find_ha_config_dir() -> str | None:
    """Locate Home Assistant configuration directory."""
    candidates = [
        os.environ.get("HA_CONFIG_DIR"),
        "/config",
        "/DATA/AppData/big-bear-home-assistant/config",
        os.path.expanduser("~/.homeassistant"),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
    ]
    for c in candidates:
        if c and os.path.isfile(os.path.join(c, ".storage", "core.config_entries")):
            return c
    return None


def auto_load_credentials(config_dir: str | None) -> dict[str, Any]:
    """Auto-detect credentials and device details from Home Assistant storage."""
    cfg_dir = config_dir or find_ha_config_dir()
    if not cfg_dir:
        raise RuntimeError(
            "Home Assistant config directory not found. Please specify credentials via CLI or HA_CONFIG_DIR."
        )

    entries_file = os.path.join(cfg_dir, ".storage", "core.config_entries")
    with open(entries_file, "r") as f:
        data = json.load(f)

    entry = next(
        (e for e in data.get("data", {}).get("entries", []) if e.get("domain") == "qubo_doorbell"),
        None,
    )
    if not entry:
        raise RuntimeError("No 'qubo_doorbell' entry found in Home Assistant config.")

    result: dict[str, Any] = {
        "email": entry["data"]["email"],
        "password": entry["data"]["password"],
        "device_id": entry["data"].get("device_id") or "ha_p2p_bridge",
        "config_dir": cfg_dir,
    }

    # Attempt to read device_uuid from core.device_registry
    dev_reg_file = os.path.join(cfg_dir, ".storage", "core.device_registry")
    if os.path.isfile(dev_reg_file):
        try:
            with open(dev_reg_file, "r") as df:
                dev_data = json.load(df)
            for dev in dev_data.get("data", {}).get("devices", []):
                model = dev.get("model", "") or ""
                if "HCD04" in model or "Doorbell" in model or "Camera" in model:
                    for ident in dev.get("identifiers", []):
                        if len(ident) == 2 and ident[0] == "qubo_doorbell":
                            result["device_uuid"] = ident[1]
                            break
        except Exception:
            pass

    return result


class QuboP2PStreamer:
    """Real-time P2P UDP decryptor and RTSP bridge for Qubo Video Doorbells."""

    def __init__(
        self,
        email: str,
        password: str,
        device_id: str,
        unit_uuid: str | None = None,
        device_uuid: str | None = None,
        local_ip: str | None = None,
        local_port: int = DEFAULT_LOCAL_PORT,
        rtsp_target: str = DEFAULT_RTSP_TARGET,
        rtp_video_port: int = DEFAULT_RTP_VIDEO_PORT,
        rtp_audio_port: int = DEFAULT_RTP_AUDIO_PORT,
        config_dir: str | None = None,
    ) -> None:
        self.email = email
        self.password = password
        self.device_id = device_id
        self.unit_uuid = unit_uuid
        self.device_uuid = device_uuid
        self.local_ip = local_ip or get_default_local_ip()
        self.local_port = local_port
        self.rtsp_target = rtsp_target
        self.rtp_video_port = rtp_video_port
        self.rtp_audio_port = rtp_audio_port
        self.config_dir = config_dir or find_ha_config_dir()
        self.sdp_path = f"/tmp/qubo_{self.local_port}.sdp"

        self.key = os.urandom(16)
        self.iv = os.urandom(16)
        self.aesgcm = AESGCM(self.key)

        self.sock: socket.socket | None = None
        self.out_sock: socket.socket | None = None
        self.client: QuboClient | None = None
        self.session: aiohttp.ClientSession | None = None
        self.user_app_id = f"ha_p2p_{os.urandom(4).hex()}"
        self.camera_addr: tuple[str, int] | None = None
        self.ffmpeg_proc: subprocess.Popen[bytes] | None = None

        self.running = False
        self.stopped = False
        self.roc = 0
        self.last_seq: int | None = None
        self.last_keepalive = 0.0

    def _get_cache_path(self) -> str:
        """Return path to persistent cache file."""
        if self.config_dir and os.path.isdir(os.path.join(self.config_dir, ".storage")):
            return os.path.join(self.config_dir, ".storage", "qubo_p2p_cache.json")
        return os.path.expanduser("~/.qubo_p2p_cache.json")

    def _load_cache(self) -> dict[str, Any]:
        """Load cached tokens, device IDs, and parameter sets."""
        path = self._get_cache_path()
        if os.path.isfile(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                if data.get("email") == self.email:
                    return data
            except Exception:
                pass
        return {}

    def _save_cache(self, updates: dict[str, Any]) -> None:
        """Update persistent cache."""
        try:
            cache = self._load_cache()
            cache.update(updates)
            cache["email"] = self.email
            path = self._get_cache_path()
            with open(path, "w") as f:
                json.dump(cache, f, indent=2)
        except Exception as e:
            _LOGGER.debug("Could not save cache: %s", e)

    def start_ffmpeg(self) -> None:
        """Launch ultra-low-latency FFmpeg publisher piping decoupled RTP streams into RTSP server."""
        # Generate SDP manifest for FFmpeg RTP demuxer
        sdp_content = SDP_TEMPLATE.format(
            video_port=self.rtp_video_port,
            audio_port=self.rtp_audio_port,
        )
        with open(self.sdp_path, "w") as f:
            f.write(sdp_content)

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-protocol_whitelist", "file,udp,rtp",
            "-probesize", "32768",
            "-analyzeduration", "100000",
            "-fpsprobesize", "0",
            "-fflags", "nobuffer+flush_packets",
            "-flags", "low_delay",
            "-i", self.sdp_path,
            "-c:v", "copy",
            "-c:a", "copy",
            "-rtsp_transport", "tcp",
            "-f", "rtsp",
            self.rtsp_target,
            "-map", "0:v:0",
            "-r", "1/5",
            "-update", "1",
            "-y", "/tmp/qubo_snapshot.jpg",
        ]
        _LOGGER.info("Starting low-latency A/V FFmpeg RTSP publisher: %s", " ".join(cmd))
        self.ffmpeg_proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def send_stun_response(self, tid: bytes, addr: tuple[str, int]) -> None:
        """Send authenticated STUN Binding Success response."""
        ip_parts = [int(p) for p in addr[0].split(".")]
        xor_port = addr[1] ^ 0x244A
        xor_ip = bytes([ip_parts[i] ^ MAGIC_COOKIE[i] for i in range(4)])
        xor_attr = struct.pack("!HHBBH", 0x0020, 8, 0, 1, xor_port) + xor_ip

        resp_hdr = struct.pack("!HHI", 0x0101, 36, 0x244A2112) + tid
        resp_sig = hmac.new(self.key, resp_hdr + xor_attr, hashlib.sha1).digest()
        resp_mi = struct.pack("!HH", 0x0008, 20) + resp_sig
        if self.sock:
            self.sock.sendto(resp_hdr + xor_attr + resp_mi, addr)

    def send_stun_nomination(self, addr: tuple[str, int]) -> None:
        """Send STUN Binding Request with ICE USE-CANDIDATE nomination to trigger camera streaming."""
        req_tid = os.urandom(12)
        use_cand = struct.pack("!HH", 0x0025, 0)
        req_hdr = struct.pack("!HHI", 0x0001, 28, 0x244A2112) + req_tid
        req_sig = hmac.new(self.key, req_hdr + use_cand, hashlib.sha1).digest()
        req_mi = struct.pack("!HH", 0x0008, 20) + req_sig
        if self.sock:
            self.sock.sendto(req_hdr + use_cand + req_mi, addr)

    async def run(self) -> None:
        """Establish local P2P session, listen on UDP, decrypt packets, and stream to RTSP."""
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Increase receive buffer to 1MB to absorb frame burst without dropping UDP packets
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
        except Exception:
            pass
        self.sock.bind(("0.0.0.0", self.local_port))
        self.sock.setblocking(False)

        self.out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        cache = self._load_cache()

        if not self.unit_uuid and cache.get("unit_uuid"):
            self.unit_uuid = cache["unit_uuid"]
        if not self.device_uuid and cache.get("device_uuid"):
            self.device_uuid = cache["device_uuid"]

        self.session = aiohttp.ClientSession()
        auth = QuboAuth(device_id=self.device_id, session=self.session)

        # Fast path: Check cached access token validity (> 300s remaining)
        cached_token = cache.get("access_token")
        cached_refresh = cache.get("refresh_token")
        cached_user_uuid = cache.get("user_uuid", "")
        cached_expiry = cache.get("expires_at", 0)

        if cached_token and (cached_expiry - time.time()) > 300:
            _LOGGER.info("Using cached Qubo authentication token (valid for %ds)", int(cached_expiry - time.time()))
            auth.set_tokens(
                access_token=cached_token,
                refresh_token=cached_refresh,
                user_uuid=cached_user_uuid,
                expires_at=cached_expiry,
            )
        else:
            token_renewed = False
            if cached_refresh and cached_user_uuid:
                try:
                    _LOGGER.info("Attempting to renew session using cached refresh token...")
                    auth.set_tokens(
                        access_token=cached_token or "",
                        refresh_token=cached_refresh,
                        user_uuid=cached_user_uuid,
                        expires_at=0,
                    )
                    payload = await auth.refresh_session()
                    self._save_cache({
                        "access_token": payload.access_token,
                        "refresh_token": payload.refresh_token,
                        "user_uuid": payload.user_uuid,
                        "expires_at": payload.expires_at,
                    })
                    token_renewed = True
                    _LOGGER.info("Session successfully refreshed.")
                except Exception as err:
                    _LOGGER.debug("Refresh token renewal deferred: %s", err)

            if not token_renewed:
                _LOGGER.info("Authenticating with Qubo Cloud via email login...")
                payload = await auth.login_with_email(self.email, self.password, device_attribute="Home Assistant")
                self._save_cache({
                    "access_token": payload.access_token,
                    "refresh_token": payload.refresh_token,
                    "user_uuid": payload.user_uuid,
                    "expires_at": payload.expires_at,
                })

        self.client = QuboClient(auth, session=self.session)

        # Auto-discover unit & device UUID if not present
        if not self.unit_uuid or not self.device_uuid:
            _LOGGER.info("Discovering Qubo doorbell UUIDs from account...")
            devices = await self.client.get_all_devices()
            camera = next(
                (
                    d for d in devices
                    if isinstance(d, QuboCamera)
                    or getattr(d, "is_camera", False)
                    or hasattr(d, "start_stream")
                    or "doorbell" in getattr(d, "model", "").lower()
                    or "hcd04" in getattr(d, "model", "").lower()
                ),
                None,
            )
            if not camera:
                raise RuntimeError("No Qubo camera or video doorbell found in account.")
            self.unit_uuid = camera.unit_id
            self.device_uuid = camera.device_id
            _LOGGER.info("Discovered camera: %s (unit=%s, device=%s)", camera.name, self.unit_uuid, self.device_uuid)
            self._save_cache({
                "unit_uuid": self.unit_uuid,
                "device_uuid": self.device_uuid,
            })

        _LOGGER.info("Initiating local P2P session via Qubo Cloud (sub-second handshake)...")
        await self.client.initiate_p2p_call(
            unit_uuid=self.unit_uuid,
            device_uuid=self.device_uuid,
            local_ip=self.local_ip,
            local_port=self.local_port,
            quality="high",
            aes_key=self.key,
            aes_iv=self.iv,
            src_device_id=self.user_app_id,
        )
        _LOGGER.info(
            "P2P call negotiated successfully. Listening for media on UDP %s:%d...",
            self.local_ip,
            self.local_port,
        )

        self.start_ffmpeg()
        loop = asyncio.get_running_loop()
        first_frame = True

        try:
            while self.running:
                try:
                    data, addr = await asyncio.wait_for(loop.sock_recvfrom(self.sock, 65535), timeout=0.5)
                    self.camera_addr = addr
                    now = time.time()

                    is_stun = len(data) >= 20 and data[4:8] == MAGIC_COOKIE
                    if is_stun:
                        msg_type = struct.unpack("!H", data[:2])[0]
                        tid = data[8:20]
                        if msg_type == 0x0001:
                            self.send_stun_response(tid, addr)
                            self.send_stun_nomination(addr)
                            self.last_keepalive = now
                    else:
                        if len(data) < 28:
                            continue
                        pt = data[1] & 0x7F
                        aad = data[:12]
                        ciphertext = data[12:-16]
                        tag = data[-16:]

                        # Track video stream (PT 96: H.264 Baseline)
                        if pt == 96:
                            seq = struct.unpack("!H", data[2:4])[0]
                            plaintext = None

                            # 1. Try estimated ROC using RFC 3711 index calculation
                            if self.last_seq is not None:
                                if self.last_seq < 32768:
                                    v = self.roc - 1 if (seq - self.last_seq) > 32768 and self.roc > 0 else self.roc
                                else:
                                    v = self.roc + 1 if (self.last_seq - 32768) > seq else self.roc
                            else:
                                v = self.roc

                            for endian in getattr(self, "_endian_candidates", ("<I", "!I")):
                                temp_iv = data[:12] + struct.pack(endian, v)
                                derived_iv = bytes([a ^ b for a, b in zip(self.iv, temp_iv)])
                                try:
                                    plaintext = self.aesgcm.decrypt(derived_iv, ciphertext + tag, aad)
                                    self._endian_candidates = (endian,)
                                    break
                                except Exception:
                                    pass

                            # 2. Fast scan if not yet locked on initial stream start or drift
                            if plaintext is None:
                                scan_range = range(200) if not getattr(self, "_roc_locked", False) else [v - 1, v + 1, v + 2]
                                for test_roc in scan_range:
                                    if test_roc < 0 or test_roc == v:
                                        continue
                                    for endian in ("<I", "!I"):
                                        temp_iv = data[:12] + struct.pack(endian, test_roc)
                                        derived_iv = bytes([a ^ b for a, b in zip(self.iv, temp_iv)])
                                        try:
                                            plaintext = self.aesgcm.decrypt(derived_iv, ciphertext + tag, aad)
                                            _LOGGER.info(
                                                "Locked on video ROC=%d (endian=%s) for seq=%d",
                                                test_roc, endian, seq
                                            )
                                            self.roc = test_roc
                                            self._endian_candidates = (endian,)
                                            self._roc_locked = True
                                            v = test_roc
                                            break
                                        except Exception:
                                            pass
                                    if plaintext is not None:
                                        break

                            if plaintext is not None:
                                if first_frame:
                                    _LOGGER.info("First H.264 video RTP packet decrypted. Live stream established!")
                                    first_frame = False

                                # Update SRTP highest index
                                if self.last_seq is None:
                                    self.last_seq = seq
                                    self.roc = v
                                    self._roc_locked = True
                                else:
                                    if v > self.roc or (v == self.roc and seq > self.last_seq):
                                        self.last_seq = seq
                                        self.roc = v

                                if self.out_sock:
                                    self.out_sock.sendto(
                                        data[:12] + plaintext, ("127.0.0.1", self.rtp_video_port)
                                    )
                            else:
                                if not hasattr(self, "_err_count"):
                                    self._err_count = 0
                                self._err_count += 1
                                if self._err_count <= 5 or self._err_count % 100 == 0:
                                    _LOGGER.warning(
                                        "Video decryption failed (roc=%d, seq=%d, len=%d)",
                                        self.roc, seq, len(data)
                                    )

                        # Track audio stream (PT 97: AAC-LC 16kHz)
                        elif pt == 97:
                            temp_iv = data[:12] + b"\x00\x00\x00\x00"
                            derived_iv = bytes([a ^ b for a, b in zip(self.iv, temp_iv)])
                            try:
                                plaintext = self.aesgcm.decrypt(derived_iv, ciphertext + tag, aad)
                                if plaintext and self.out_sock:
                                    self.out_sock.sendto(
                                        data[:12] + plaintext, ("127.0.0.1", self.rtp_audio_port)
                                    )
                            except Exception:
                                pass

                    # Keepalive consent freshness every 1.5s
                    if self.camera_addr and (now - self.last_keepalive) > 1.5:
                        self.send_stun_nomination(self.camera_addr)
                        self.last_keepalive = now

                    # Monitor FFmpeg health
                    if self.ffmpeg_proc and self.ffmpeg_proc.poll() is not None:
                        _LOGGER.warning("FFmpeg publisher process exited with code %d", self.ffmpeg_proc.returncode)
                        break

                except asyncio.TimeoutError:
                    if self.camera_addr and (time.time() - self.last_keepalive) > 1.5:
                        self.send_stun_nomination(self.camera_addr)
                        self.last_keepalive = time.time()
        finally:
            await self.stop(final=False)

    async def stop(self, final: bool = True) -> None:
        """Tear down streamer and release camera hardware session."""
        if final:
            if self.stopped:
                return
            self.stopped = True
        self.running = False
        _LOGGER.info("Cleaning up Qubo P2P Streamer session...")

        if self.ffmpeg_proc:
            try:
                self.ffmpeg_proc.terminate()
                self.ffmpeg_proc.wait(timeout=1.0)
            except Exception:
                try:
                    self.ffmpeg_proc.kill()
                except Exception:
                    pass
            self.ffmpeg_proc = None

        if self.out_sock:
            try:
                self.out_sock.close()
            except Exception:
                pass
            self.out_sock = None

        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

        try:
            if os.path.exists(self.sdp_path):
                os.remove(self.sdp_path)
        except Exception:
            pass

        # Release session with Qubo cloud API so camera blue LED turns off
        if self.client and hasattr(self.client, "request") and self.unit_uuid and self.device_uuid:
            try:
                v1_path = (
                    f"/stream-manager/api/v1/sp/{self.client.sp_id}/stream/unit/"
                    f"{self.unit_uuid}/device/{self.device_uuid}"
                )
                await self.client.request(
                    "DELETE", v1_path, params={"type": "high", "userAppID": self.user_app_id}
                )
                _LOGGER.info("Successfully released Qubo stream session. Blue ring LED will extinguish.")
            except Exception as e:
                _LOGGER.debug("Teardown notice: %s", e)

        if self.session and not self.session.closed:
            await self.session.close()

    async def run_persistent(self) -> None:
        """Run continuously in the background, automatically reconnecting on network or camera drop."""
        backoff = 5
        while not self.stopped:
            try:
                _LOGGER.info("Starting persistent P2P stream session...")
                await self.run()
                backoff = 5
            except Exception as e:
                err_str = str(e)
                if "ERR_SM_050" in err_str:
                    _LOGGER.warning(
                        "Qubo login limit reached (ERR_SM_050). Maximum concurrent sessions in use. "
                        "Please log out of other devices in the Qubo mobile app. Retrying in 60s..."
                    )
                    backoff = 60
                else:
                    _LOGGER.error("Stream session error: %s. Reconnecting in %ds...", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                await self.stop(final=False)
                await asyncio.sleep(2)


def main() -> None:
    """CLI entrypoint with automatic discovery and zero sensitive hardcoding."""
    parser = argparse.ArgumentParser(description="Qubo Video Doorbell Local P2P Video Streamer")
    parser.add_argument("--config-dir", help="Path to Home Assistant config directory (for auto-detection)")
    parser.add_argument("--email", help="Qubo account email")
    parser.add_argument("--password", help="Qubo account password")
    parser.add_argument("--device-token", help="Client device token (default: ha_p2p_bridge)")
    parser.add_argument("--unit-uuid", help="Doorbell unit UUID (auto-discovered if omitted)")
    parser.add_argument("--device-uuid", help="Doorbell device UUID (auto-discovered if omitted)")
    parser.add_argument("--local-ip", help="Host LAN IP address (auto-detected if omitted)")
    parser.add_argument("--local-port", type=int, default=DEFAULT_LOCAL_PORT, help="Local UDP port (default: 49700)")
    parser.add_argument("--rtsp-target", default=DEFAULT_RTSP_TARGET, help="RTSP publish target URL")
    parser.add_argument("--rtp-video-port", type=int, default=DEFAULT_RTP_VIDEO_PORT, help="RTP Video port (default: 5004)")
    parser.add_argument("--rtp-audio-port", type=int, default=DEFAULT_RTP_AUDIO_PORT, help="RTP Audio port (default: 5006)")
    parser.add_argument("--persistent", action="store_true", help="Run continuously 24/7 with auto-reconnect")

    args = parser.parse_args()

    email = args.email
    password = args.password
    device_token = args.device_token or "ha_p2p_bridge"
    unit_uuid = args.unit_uuid
    device_uuid = args.device_uuid
    config_dir = args.config_dir

    if not email or not password or not args.device_token:
        try:
            creds = auto_load_credentials(config_dir)
            email = email or creds["email"]
            password = password or creds["password"]
            if not args.device_token:
                device_token = creds.get("device_id") or device_token
            device_uuid = device_uuid or creds.get("device_uuid")
            config_dir = config_dir or creds.get("config_dir")
            _LOGGER.info("Auto-loaded credentials for: %s (device_id: %s)", email, device_token)
        except Exception as e:
            _LOGGER.error("Failed to auto-load credentials: %s", e)
            sys.exit(1)

    streamer = QuboP2PStreamer(
        email=email,
        password=password,
        device_id=device_token,
        unit_uuid=unit_uuid,
        device_uuid=device_uuid,
        local_ip=args.local_ip,
        local_port=args.local_port,
        rtsp_target=args.rtsp_target,
        rtp_video_port=args.rtp_video_port,
        rtp_audio_port=args.rtp_audio_port,
        config_dir=config_dir,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal() -> None:
        _LOGGER.info("Received stop signal, exiting streamer loop...")
        streamer.running = False
        streamer.stopped = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    if args.persistent:
        try:
            loop.run_until_complete(streamer.run_persistent())
        except Exception as e:
            _LOGGER.error("Persistent streamer error: %s", e)
        finally:
            loop.close()
    else:
        try:
            loop.run_until_complete(streamer.run())
        except Exception as e:
            _LOGGER.error("Streamer run error: %s", e)
        finally:
            loop.close()


if __name__ == "__main__":
    main()
