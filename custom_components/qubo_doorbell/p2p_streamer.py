#!/usr/bin/env python3
"""Qubo Video Doorbell Local P2P Video Streamer.

Direct LAN media bridge: Doorbell (UDP) -> Host -> MediaMTX / go2rtc (RTSP)
- Negotiates P2P UDP candidate via Qubo Cloud API (p2pCall)
- Handles ICE/STUN binding & USE-CANDIDATE nomination
- Maintains ICE consent freshness keepalives
- Decrypts AES-128-GCM RTP video payload (Dynamic PT 96) in real-time
- Reassembles H.264 Annex B NAL units (FU-A and single NALs)
- Direct low-latency passthrough into RTSP server via FFmpeg
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

# Default SPS/PPS for Qubo Video Doorbell Pro (HCD04C 2K) to eliminate probe latency
DEFAULT_SPS = bytes.fromhex("000000016742c028d900540507a0220000030002000003007908")
DEFAULT_PPS = bytes.fromhex("0000000168ce3c80")


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
        self.config_dir = config_dir or find_ha_config_dir()

        self.key = os.urandom(16)
        self.iv = os.urandom(16)
        self.aesgcm = AESGCM(self.key)

        self.sock: socket.socket | None = None
        self.client: QuboClient | None = None
        self.session: aiohttp.ClientSession | None = None
        self.user_app_id = f"ha_p2p_{os.urandom(4).hex()}"
        self.camera_addr: tuple[str, int] | None = None
        self.ffmpeg_proc: subprocess.Popen[bytes] | None = None

        self.running = False
        self.stopped = False
        self.roc = 0
        self.last_seq: int | None = None
        self.fu_buffer = bytearray()
        self.last_keepalive = 0.0

        # SPS / PPS state
        self.sps = DEFAULT_SPS
        self.pps = DEFAULT_PPS

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
        """Launch low-latency FFmpeg publisher piping Annex B stream into RTSP server."""
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-probesize", "4096",
            "-analyzeduration", "50000",
            "-fpsprobesize", "0",
            "-fflags", "nobuffer+flush_packets",
            "-flags", "low_delay",
            "-use_wallclock_as_timestamps", "1",
            "-f", "h264",
            "-r", "10",
            "-i", "pipe:0",
            "-c:v", "copy",
            "-an",
            "-muxdelay", "0.01",
            "-max_interleave_delta", "0",
            "-rtsp_transport", "tcp",
            "-f", "rtsp",
            self.rtsp_target,
        ]
        _LOGGER.info("Starting low-latency FFmpeg RTSP publisher: %s", " ".join(cmd))
        self.ffmpeg_proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Pre-seed FFmpeg with SPS + PPS so probe succeeds in <50ms without waiting for camera IDR
        if self.sps and self.pps and self.ffmpeg_proc and self.ffmpeg_proc.stdin:
            try:
                self.ffmpeg_proc.stdin.write(self.sps + self.pps)
                self.ffmpeg_proc.stdin.flush()
                _LOGGER.debug("Pre-seeded FFmpeg with SPS/PPS for instant probe")
            except Exception as e:
                _LOGGER.debug("Pre-seeding notice: %s", e)

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

        cache = self._load_cache()
        if cache.get("sps_b64") and cache.get("pps_b64"):
            try:
                self.sps = base64.b64decode(cache["sps_b64"])
                self.pps = base64.b64decode(cache["pps_b64"])
            except Exception:
                pass

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
            _LOGGER.info("Authenticating with Qubo Cloud...")
            payload = await auth.login_with_email(self.email, self.password, device_attribute="LocalP2PBridge")
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
                        seq = struct.unpack("!H", data[2:4])[0]

                        # Rollover counter tracking
                        if self.last_seq is not None:
                            if seq < self.last_seq and (self.last_seq - seq) > 30000:
                                self.roc += 1
                        self.last_seq = seq

                        if pt != 96:
                            continue

                        # AES-128-GCM Decryption
                        aad = data[:12]
                        ciphertext = data[12:-16]
                        tag = data[-16:]

                        temp_iv = data[:12] + struct.pack("!I", self.roc)
                        derived_iv = bytes([a ^ b for a, b in zip(self.iv, temp_iv)])

                        try:
                            plaintext = self.aesgcm.decrypt(derived_iv, ciphertext + tag, aad)
                            if not plaintext:
                                continue

                            nal_header = plaintext[0]
                            nal_type = nal_header & 0x1F

                            if nal_type == 7:  # SPS
                                self.sps = b"\x00\x00\x00\x01" + plaintext
                                self._save_cache({"sps_b64": base64.b64encode(self.sps).decode("ascii")})
                            elif nal_type == 8:  # PPS
                                self.pps = b"\x00\x00\x00\x01" + plaintext
                                self._save_cache({"pps_b64": base64.b64encode(self.pps).decode("ascii")})

                            if 1 <= nal_type <= 23:
                                # Single NAL Unit
                                if first_frame:
                                    _LOGGER.info(
                                        "First H.264 NAL received (type=%d, len=%d). Live stream established!",
                                        nal_type,
                                        len(plaintext),
                                    )
                                    first_frame = False
                                if self.ffmpeg_proc and self.ffmpeg_proc.stdin:
                                    self.ffmpeg_proc.stdin.write(b"\x00\x00\x00\x01" + plaintext)
                                    self.ffmpeg_proc.stdin.flush()
                            elif nal_type == 28:  # FU-A fragmented slice
                                fu_indicator = nal_header
                                fu_header = plaintext[1]
                                start_bit = fu_header & 0x80
                                end_bit = fu_header & 0x40
                                fu_type = fu_header & 0x1F

                                if start_bit:
                                    self.fu_buffer = bytearray(b"\x00\x00\x00\x01")
                                    reconstructed_nal = (fu_indicator & 0xE0) | fu_type
                                    self.fu_buffer.append(reconstructed_nal)
                                    self.fu_buffer.extend(plaintext[2:])
                                else:
                                    if self.fu_buffer:
                                        self.fu_buffer.extend(plaintext[2:])

                                if end_bit and self.fu_buffer:
                                    if first_frame:
                                        _LOGGER.info(
                                            "First fragmented NAL reassembled (type=%d, len=%d). Live stream established!",
                                            fu_type,
                                            len(self.fu_buffer),
                                        )
                                        first_frame = False
                                    if self.ffmpeg_proc and self.ffmpeg_proc.stdin:
                                        self.ffmpeg_proc.stdin.write(self.fu_buffer)
                                        self.ffmpeg_proc.stdin.flush()
                                    self.fu_buffer = bytearray()

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
            await self.stop()

    async def stop(self) -> None:
        """Tear down streamer and release camera hardware session."""
        if self.stopped:
            return
        self.stopped = True
        self.running = False
        _LOGGER.info("Stopping Qubo P2P Streamer and releasing hardware encoder...")

        if self.ffmpeg_proc:
            try:
                if self.ffmpeg_proc.stdin:
                    self.ffmpeg_proc.stdin.close()
                self.ffmpeg_proc.terminate()
                self.ffmpeg_proc.wait(timeout=2.0)
            except Exception:
                pass

        if self.sock:
            try:
                self.sock.close()
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

    args = parser.parse_args()

    email = args.email
    password = args.password
    device_token = args.device_token or "ha_p2p_bridge"
    unit_uuid = args.unit_uuid
    device_uuid = args.device_uuid
    config_dir = args.config_dir

    if not email or not password:
        try:
            creds = auto_load_credentials(config_dir)
            email = email or creds["email"]
            password = password or creds["password"]
            device_token = creds.get("device_id") or device_token
            device_uuid = device_uuid or creds.get("device_uuid")
            config_dir = config_dir or creds.get("config_dir")
            _LOGGER.info("Auto-loaded credentials for: %s", email)
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
        config_dir=config_dir,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal() -> None:
        _LOGGER.info("Received stop signal, exiting streamer loop...")
        streamer.running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        loop.run_until_complete(streamer.run())
    except Exception as e:
        _LOGGER.error("Streamer run error: %s", e)
    finally:
        loop.close()


if __name__ == "__main__":
    main()
