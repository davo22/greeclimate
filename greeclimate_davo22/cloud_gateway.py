"""Gree Cloud device gateway (legacy, pre-MQTT transport)

The Gree+ app only talks MQTT to Wi-Fi modules whose protocol version (the
``ver`` field of the cloud device record, e.g. ``V3.5.0``) is 3.2 or newer.
Older modules (``V2.0.0``, ``V1.x``) never join the MQTT broker at all: they
hold a persistent TLS session to a per-device "iot gateway", and the app
reaches them through that gateway with a one-shot, line-oriented protocol:

1. ``POST /App/QueryOnline`` with the MAC list -> for each online device the
   gateway it is attached to, as ``svr`` (``host:port``) and ``isSSL``.
2. Open a TCP (TLS if ``isSSL``) connection to ``svr``; the certificate is
   issued by Gree's own CA and is not verifiable, the app does not verify it
   either.
3. Send one newline-terminated JSON line in the usual Gree envelope
   ``{"cid":"app","i":0,"pack":...,"t":"pack","tcid":<mac>,"uid":<uid>}``.
   The inner ``pack`` is the same ``status``/``cmd`` JSON as on the LAN, but
   always AES-128-ECB with the device key -- even when the LAN protocol for
   the same device would use GCM.
4. Read one line back (same envelope, ``pack`` decrypts to the usual
   ``{"t":"dat", "cols":[...], "dat":[...]}`` / ``{"t":"res", ...}``) and close.

Protocol worked out by jose1711 in
https://github.com/davo22/homeassistant-gree-cloud/issues/21.
"""

import asyncio
import json
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from greeclimate_davo22.cipher import CipherV1
from greeclimate_davo22.cloud_api import CloudDeviceServer, GreeCloudApi
from greeclimate_davo22.exceptions import DeviceOfflineError

_LOGGER = logging.getLogger(__name__)

# Protocol version from which the Gree+ app uses MQTT instead of the gateway.
MQTT_MIN_PROTOCOL_VERSION: Tuple[int, int] = (3, 2)


def parse_protocol_version(version: Optional[str]) -> Optional[Tuple[int, ...]]:
    """Parse a module version string such as ``V2.0.0`` into ``(2, 0, 0)``.

    Returns None when the string carries no parsable number, so callers can
    fall back to the default transport rather than guess.
    """
    if not version:
        return None
    text = version.strip().lstrip("vV")
    parts = []
    for chunk in text.split("."):
        digits = ""
        for ch in chunk:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts) if parts else None


def uses_mqtt(version: Optional[str]) -> bool:
    """Return True if a module with this ``ver`` is reachable over MQTT.

    Unknown or unparsable versions default to MQTT, which was the only
    transport before this module existed.
    """
    parsed = parse_protocol_version(version)
    if parsed is None:
        return True
    return parsed >= MQTT_MIN_PROTOCOL_VERSION


@dataclass
class _CachedServer:
    server: CloudDeviceServer
    expires: float


class GreeCloudGateway:
    """One-shot request/response client for the legacy Gree device gateway.

    One instance is shared by all devices of a cloud account: it holds the
    ``QueryOnline`` cache and the TLS context, and reads ``uid`` from the
    (logged-in) :class:`GreeCloudApi` it wraps.
    """

    def __init__(
        self,
        api: GreeCloudApi,
        timeout: float = 10.0,
        server_cache_ttl: float = 300.0,
    ) -> None:
        self._api = api
        self._timeout = timeout
        self._cache_ttl = server_cache_ttl
        self._servers: Dict[str, _CachedServer] = {}
        self._lock = asyncio.Lock()

        # Gree signs the gateway certificate with its own CA, so verification
        # is off (the app does not verify either). Build the context directly
        # instead of via create_default_context(), whose load_default_certs()
        # does blocking file I/O inside the event loop.
        self._tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._tls_context.check_hostname = False
        self._tls_context.verify_mode = ssl.CERT_NONE

    async def server_for(self, mac: str, refresh: bool = False) -> CloudDeviceServer:
        """Return the gateway the device is currently attached to.

        Raises DeviceOfflineError when the cloud does not list it as online.
        """
        now = time.monotonic()
        cached = self._servers.get(mac)
        if cached and cached.expires > now and not refresh:
            return cached.server

        async with self._lock:
            cached = self._servers.get(mac)
            if cached and cached.expires > now and not refresh:
                return cached.server
            online = await self._api.query_online([mac])
            server = online.get(mac)
            if server is None:
                self._servers.pop(mac, None)
                raise DeviceOfflineError(f"Device {mac} is offline (QueryOnline)")
            self._servers[mac] = _CachedServer(server, now + self._cache_ttl)
            _LOGGER.debug("Gateway for %s: %s:%s ssl=%s", mac, server.host, server.port, server.ssl)
            return server

    def forget(self, mac: str) -> None:
        """Drop the cached gateway for a device (e.g. after a failed connect)."""
        self._servers.pop(mac, None)

    def build_envelope(self, mac: str, cipher: CipherV1, command: Dict[str, Any]) -> bytes:
        """Encrypt *command* into the newline-terminated wire envelope."""
        encrypted = cipher.encrypt(command)
        pack = encrypted[0] if isinstance(encrypted, tuple) else encrypted
        message = {
            "cid": "app",
            "i": 0,
            "pack": pack,
            "t": "pack",
            "tcid": mac,
            "uid": self._api.user_id,
        }
        return json.dumps(message, separators=(",", ":")).encode() + b"\n"

    @staticmethod
    def parse_reply(raw: bytes, cipher: CipherV1) -> Dict[str, Any]:
        """Decode one reply line; returns the decrypted inner pack."""
        outer = json.loads(raw.decode().strip())
        if "pack" not in outer:
            return outer
        return cipher.decrypt(outer["pack"])

    async def request(self, mac: str, cipher: CipherV1, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send one command to the device and return the decrypted reply.

        Raises asyncio.TimeoutError (or DeviceOfflineError) when the device
        cannot be reached or does not answer within the timeout.
        """
        server = await self.server_for(mac)
        payload = self.build_envelope(mac, cipher, command)
        _LOGGER.debug("Gateway request to %s via %s:%s: %s", mac, server.host, server.port, command.get("t"))

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    server.host,
                    server.port,
                    ssl=self._tls_context if server.ssl else None,
                ),
                timeout=self._timeout,
            )
        except (OSError, asyncio.TimeoutError) as err:
            # The device may have moved to another gateway; re-resolve next time.
            self.forget(mac)
            raise asyncio.TimeoutError(
                f"Could not connect to gateway {server.host}:{server.port} for {mac}: {err}"
            ) from err

        try:
            writer.write(payload)
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=self._timeout)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - best effort
                pass

        if not line:
            self.forget(mac)
            raise asyncio.TimeoutError(f"Gateway closed the connection without a reply for {mac}")

        reply = self.parse_reply(line, cipher)
        _LOGGER.debug("Gateway reply from %s: %s", mac, reply)
        return reply
