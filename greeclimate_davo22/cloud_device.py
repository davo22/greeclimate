"""Cloud-based Gree device implementation

Handles communication with Gree devices through the Gree cloud. Two
transports exist, picked per device from its protocol version (``ver``):

- ``mqtt``: modules with protocol >= 3.2 are reached over the cloud MQTT
  broker (request/response/status topics).
- ``gateway``: older modules never join the broker. They are reached through
  the per-device TLS gateway returned by ``QueryOnline`` (see
  ``cloud_gateway.py``), one request/response per connection.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List

from greeclimate_davo22.cipher import CipherV1, CipherV2
from greeclimate_davo22.cloud_gateway import GreeCloudGateway, uses_mqtt
from greeclimate_davo22.device import Device, Props
from greeclimate_davo22.deviceinfo import DeviceInfo
from greeclimate_davo22.exceptions import DeviceOfflineError
from greeclimate_davo22.mqtt_client import GreeMqttClient, MqttDeviceMessage
from greeclimate_davo22.taskable import Taskable

TRANSPORT_AUTO = "auto"
TRANSPORT_MQTT = "mqtt"
TRANSPORT_GATEWAY = "gateway"

# How a sub-device (indoor unit behind a commercial module) is addressed.
# "tcid": publish to the module topic with the unit id as ``tcid`` and a plain
#         pack. Works on single-head commercial modules (verified).
# "pack": publish to the module topic with the module as ``tcid`` and the unit
#         named inside the encrypted pack (``"mac"``). This is how the Gree+ app
#         drives multi-head/VRF systems (#33); used when "tcid" gets no answer.
ADDRESSING_TCID = "tcid"
ADDRESSING_PACK = "pack"

# Keys that identify the indoor unit a reply belongs to on a shared module topic.
_REPLY_UNIT_KEYS = ("sub", "mac")

# Units currently instantiated behind each module: {module_mac: {unit_mac, ...}}.
# A reply is only dropped when it names one of *these* other units; modules
# that put an internal id in "mac" (U-WB05WR11: "000000221859" with the cloud
# MAC in "cid") must not be mistaken for someone else's reply (#66).
_UNITS_BY_MODULE: Dict[str, set] = {}

# Per-command flag: 1 = do not beep for this command. Not a reported property.
BUZZER_FLAG = "Buzzer_ON_OFF"

# Legacy gateway: how a request for a unit behind a module is addressed.
# ("route", "tcid", inner-mac): route = whose gateway QueryOnline resolves,
# tcid = who the envelope names, inner-mac = whether the pack carries "mac".
# Tried in this order the first time a unit is reached (#6, #44).
GATEWAY_VIA_UNIT = "unit"           # QueryOnline(unit), tcid unit, mac in pack
GATEWAY_VIA_MODULE = "module"       # QueryOnline(module), tcid module, mac in pack
GATEWAY_VIA_MODULE_AS_UNIT = "module-as-unit"  # QueryOnline(module), tcid unit, mac in pack
GATEWAY_MODULE_PLAIN = "module-plain"          # QueryOnline(module), tcid module, no mac
_GATEWAY_SUB_DEVICE_VARIANTS = (
    GATEWAY_VIA_UNIT,
    GATEWAY_VIA_MODULE,
    GATEWAY_VIA_MODULE_AS_UNIT,
    GATEWAY_MODULE_PLAIN,
)

# Short status request used to find out whether a module answers at all; old
# controllers on the line protocol may drop the ~80-column request (#6).
GATEWAY_PROBE_COLS = ["Pow", "Mod", "SetTem", "TemSen", "WdSpd"]
# Chunk size for modules that answer the short request but not the full one.
GATEWAY_COLS_CHUNK = 16

# What the first unit behind a module learned, shared with its siblings so
# they skip the search: {module_mac: {"transport": ..., "gateway_variant": ...,
# "gateway_cols_limit": ...}}.
_MODULE_HINTS: Dict[str, Dict[str, Any]] = {}

# One request at a time per module on the legacy gateway, with a pause
# between them: four units polling an old controller at the same moment,
# each opening its own connections, is what made it stop answering (#6).
_MODULE_GATEWAY_LOCKS: Dict[str, asyncio.Lock] = {}
_MODULE_GATEWAY_LAST: Dict[str, float] = {}
GATEWAY_REQUEST_SPACING = 0.5  # seconds between requests to one module
GATEWAY_MIN_CHUNK = 5

_LOGGER = logging.getLogger(__name__)


class CloudDevice(Device):
    """Cloud-based Gree device
    
    Communicates with Gree devices through the Gree Cloud MQTT broker.
    Supports both CipherV1 (AES-128-ECB) and CipherV2 (AES-128-GCM).
    
    Important differences from local devices:
    - Uses MQTT (protocol >= 3.2) or the cloud TLS gateway (older) instead of UDP
    - Temperature values are NOT offset (no +40 adjustment)
    - Commands must be sent sequentially with response wait
    - Indoor units behind a commercial module are addressed through the
      module (``parent_mac`` from the cloud record's ``pmac``), replies are
      routed per unit, and the addressing mode falls back automatically
    
    Example:
        ```python
        # Login to cloud
        api = GreeCloudApi.for_server('Europe', 'user@example.com', 'password')
        await api.login()
        devices = await api.get_all_devices()
        
        # Create MQTT client
        mqtt = GreeMqttClient(api.user_id, api.token)
        await mqtt.connect()
        
        # Create cloud device
        device_info = CloudDeviceInfo(
            name=devices[0].name,
            mac=devices[0].mac,
            key=devices[0].key
        )
        device = CloudDevice(mqtt, device_info)
        
        # Use device
        await device.bind()
        await device.update_state()
        device.power = True
        device.target_temperature = 24
        await device.push_state_update()
        ```
    """
    
    def __init__(
        self,
        mqtt_client: Optional[GreeMqttClient],
        device_info: DeviceInfo,
        device_key: str,
        cipher_version: int = 1,
        timeout: int = 120,
        command_timeout: int = 10,
        loop: asyncio.AbstractEventLoop = None,
        gateway: Optional[GreeCloudGateway] = None,
        transport: str = TRANSPORT_AUTO,
        parent_mac: Optional[str] = None,
    ):
        """Initialize cloud device
        
        Args:
            mqtt_client: Connected MQTT client instance (may be None for a
                device that only uses the gateway transport)
            device_info: Device information (ip/port not used, but mac/name
                required; ``version`` selects the transport in auto mode)
            device_key: Device encryption key from Cloud API
            cipher_version: Cipher version to use (1 = ECB, 2 = GCM), defaults to 1
            timeout: General timeout for device operations
            command_timeout: Timeout for MQTT command responses
            loop: Event loop
            gateway: Shared GreeCloudGateway for pre-3.2 modules. Without it
                every device uses MQTT, as before.
            transport: "auto" (from ``device_info.version``), "mqtt" or "gateway"
            parent_mac: MAC of the module this unit sits behind, as reported by
                the cloud record (``pmac``). Without it the module is guessed
                by stripping a trailing ``00`` from the unit id, which is only
                right for single-head modules.
        """
        super().__init__(device_info, timeout=timeout, loop=loop)
        
        self._mqtt_client = mqtt_client
        self._gateway = gateway
        self._device_key = device_key
        self._cipher_version = cipher_version
        self._command_timeout = command_timeout
        
        # Setup cipher based on version
        if cipher_version == 2:
            self.device_cipher = CipherV2(device_key.encode())
        else:
            self.device_cipher = CipherV1(device_key.encode())
        
        # Parent (module) / child (unit) addressing. The cloud record's pmac
        # is authoritative; the "strip 00" guess is wrong for multi-head
        # systems, whose unit ids do not derive from the module MAC (#6).
        self._child_mac = device_info.mac
        self._parent_mac = (parent_mac or "").strip() or self._detect_parent_mac(device_info.mac)
        self._addressing = ADDRESSING_TCID
        _UNITS_BY_MODULE.setdefault(self._parent_mac, set()).add(self._child_mac)
        # Legacy gateway: addressing variant and column limit, learned on the
        # first exchange (or taken from a sibling behind the same module).
        hints = _MODULE_HINTS.get(self._parent_mac, {})
        self._gateway_variant: Optional[str] = hints.get("gateway_variant")
        self._gateway_cols_limit: Optional[int] = hints.get("gateway_cols_limit")

        # Confirmation beep on commands. When False, every "cmd" pack carries
        # Buzzer_ON_OFF=1 so the indoor unit stays silent (the unit does not
        # report this flag; the Gree+ app and the local LAN protocol send it
        # per command). davo22/homeassistant-gree-cloud#50
        self.beeper: bool = True
        
        # Response handling
        self._response_event: Optional[asyncio.Event] = None
        self._response_data: Optional[Dict] = None

        self.transport = self._resolve_transport(transport)
        if self.transport == TRANSPORT_MQTT and mqtt_client is None:
            raise ValueError(f"{device_info.name}: MQTT transport selected but no MQTT client given")
        
        # Setup message handler
        if self._mqtt_client is not None:
            self._mqtt_client.add_message_handler(self._handle_mqtt_message)
        
        _LOGGER.info(
            f"CloudDevice initialized: {device_info.name} (parent: {self._parent_mac}, "
            f"child: {self._child_mac}, ver: {device_info.version}, transport: {self.transport})"
        )

    @property
    def is_sub_device(self) -> bool:
        """True when this unit is addressed through a separate module (parent) MAC."""
        return self._parent_mac != self._child_mac

    @property
    def addressing(self) -> str:
        """Current sub-device addressing mode: ADDRESSING_TCID or ADDRESSING_PACK."""
        return self._addressing

    def _resolve_transport(self, requested: str) -> str:
        """Pick the transport: explicit request, else from the protocol version."""
        if requested in (TRANSPORT_MQTT, TRANSPORT_GATEWAY):
            if requested == TRANSPORT_GATEWAY and self._gateway is None:
                raise ValueError("gateway transport requested but no GreeCloudGateway given")
            return requested
        if requested != TRANSPORT_AUTO:
            raise ValueError(f"Unknown transport: {requested!r}")
        learned = _MODULE_HINTS.get(self._parent_mac, {}).get("transport")
        if learned == TRANSPORT_GATEWAY and self._gateway is not None:
            return TRANSPORT_GATEWAY
        if learned == TRANSPORT_MQTT and self._mqtt_client is not None:
            return TRANSPORT_MQTT
        if self._gateway is not None and not uses_mqtt(self.device_info.version):
            # Indoor units behind a commercial module report the module's own
            # version string (e.g. "V1.1.0.0"), which says nothing about how
            # the module is reached; the only multi-head systems known to
            # work were driven over MQTT (#33). Start there and let bind()
            # fall back to the gateway if the module stays silent (#6).
            if self.is_sub_device and self._mqtt_client is not None:
                return TRANSPORT_MQTT
            return TRANSPORT_GATEWAY
        return TRANSPORT_MQTT

    def _remember(self, **hints: Any) -> None:
        """Record what worked for this module, for the siblings behind it.

        A column limit only ever gets smaller: a sibling that is still probing
        must not undo what another one learned the hard way.
        """
        current = _MODULE_HINTS.setdefault(self._parent_mac, {})
        if "gateway_cols_limit" in hints and current.get("gateway_cols_limit") is not None:
            hints["gateway_cols_limit"] = min(hints["gateway_cols_limit"], current["gateway_cols_limit"])
        current.update(hints)

    def _other_transport(self) -> Optional[str]:
        """The transport to try when the chosen one gets no answer, if available."""
        if self.transport == TRANSPORT_MQTT:
            return TRANSPORT_GATEWAY if self._gateway is not None else None
        return TRANSPORT_MQTT if self._mqtt_client is not None else None

    @property
    def uses_mqtt(self) -> bool:
        """True if this device is driven over the MQTT broker."""
        return self.transport == TRANSPORT_MQTT

    def _gateway_cipher(self) -> CipherV1:
        """The gateway always speaks ECB with the device key, even for GCM modules."""
        if isinstance(self.device_cipher, CipherV1):
            return self.device_cipher
        return CipherV1(self._device_key.encode())
    
    def _detect_parent_mac(self, mac: str) -> str:
        """Detect parent MAC from child MAC
        
        For devices ending with '00' and longer than 12 chars, strip last 2 chars.
        This handles parent/child device hierarchy for commercial units.
        """
        if mac.endswith('00') and len(mac) > 12:
            return mac[:-2]
        return mac
    
    async def bind(self, key: str = None, cipher=None):
        """Bind to cloud device
        
        For cloud devices, binding is simplified since we already have
        the device key from the Cloud API.
        
        Args:
            key: Device key (ignored, uses key from __init__)
            cipher: Cipher (ignored, uses cipher from __init__)
        """
        # Subscribe to device topics (gateway devices have nothing to subscribe to)
        if self.uses_mqtt:
            await self._mqtt_client.subscribe_to_device(self._parent_mac)

        _LOGGER.info(f"Cloud device bound: {self.device_info.name} ({self.transport})")

        # Trigger initial state update. A device that stays silent on the
        # transport picked from its version string gets one try on the other
        # transport (when the caller provided both); whichever answers is
        # kept. Only at bind time, so a dead device costs two timeouts once,
        # not on every poll.
        try:
            await self.update_state()
        except asyncio.TimeoutError:
            other = self._other_transport()
            if other is None:
                raise
            first = self.transport
            _LOGGER.info(
                f"{self.device_info.name}: no answer over {first}, trying {other}"
            )
            self.transport = other
            if self.uses_mqtt:
                await self._mqtt_client.subscribe_to_device(self._parent_mac)
            try:
                await self.update_state()
            except asyncio.TimeoutError:
                self.transport = first
                if other == TRANSPORT_MQTT:
                    try:
                        await self._mqtt_client.unsubscribe_from_device(self._parent_mac)
                    except Exception:  # pragma: no cover - best effort
                        pass
                raise
            _LOGGER.info(f"{self.device_info.name}: answers over {other}; keeping it")
        self._remember(transport=self.transport)

    def _status_columns(self) -> List[str]:
        """Property keys requested by update_state(); subclasses may extend."""
        return [x.value for x in Props]
    
    async def update_state(self):
        """Update device state from cloud

        Raises asyncio.TimeoutError when the device does not answer, so that
        callers can count failures and mark the device unavailable. Swallowing
        it here made a dead device indistinguishable from an idle one.
        """
        _LOGGER.debug(f"Updating cloud device state: {self.device_info.name}")
        
        # Request all properties
        props = list(self._status_columns())
        if not self.hid:
            props.append('hid')

        try:
            data = await self.request_status(props)
        except asyncio.TimeoutError:
            _LOGGER.warning(f"Timeout waiting for state update from {self.device_info.name}")
            raise

        if data:
            self.handle_state_update(**data)

    async def request_status(self, cols: List[str], timeout: Optional[float] = None) -> Dict[str, Any]:
        """Ask the device for the given property columns and return them as a dict.

        Public request/response primitive. Does not touch the device state;
        use :meth:`update_state` for that. Raises ``asyncio.TimeoutError`` (or
        ``DeviceOfflineError``) when the device does not answer. A reply that
        carries no ``dat`` pack yields an empty dict.

        For a sub-device that never answers under the current addressing, the
        other addressing mode is tried once and kept if it works.
        """
        command = {'t': 'status', 'cols': list(cols)}
        timeout = self._command_timeout if timeout is None else timeout

        if not self.uses_mqtt:
            return await self._gateway_status_cols(list(cols))

        try:
            reply = await self._mqtt_request(command, timeout)
        except asyncio.TimeoutError:
            if not self.is_sub_device:
                raise
            other = ADDRESSING_PACK if self._addressing == ADDRESSING_TCID else ADDRESSING_TCID
            _LOGGER.debug(
                f"{self.device_info.name}: no answer with '{self._addressing}' addressing, "
                f"trying '{other}'"
            )
            reply = await self._mqtt_request(command, timeout, addressing=other)
            _LOGGER.info(
                f"{self.device_info.name}: unit answers with '{other}' addressing "
                f"(module {self._parent_mac}, unit {self._child_mac}); keeping it"
            )
            self._addressing = other
        return self._status_data(reply)

    def _status_data(self, reply: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Decode a status reply; say so when the device answered with something else.

        A reply that is not a ``dat`` pack (an error pack, a bare
        acknowledgement, a pack for another unit without a unit id) used to
        be swallowed as "no data", leaving the device bound with empty state
        and the user with an *unknown* entity and no clue (#6).
        """
        data = self._decode_dat(reply)
        if data:
            return data
        if reply:
            _LOGGER.warning(
                f"{self.device_info.name}: status request answered with a "
                f"{reply.get('t', '?')!r} pack instead of data: "
                f"{ {k: v for k, v in reply.items() if k != 'pack'} }"
            )
        return {}

    async def send_command(
        self,
        opt: List[str],
        p: List[Any],
        *,
        strict_ack: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Send one ``cmd`` pack (``opt``/``p`` lists) and return the decrypted reply.

        Public command primitive for callers that need the acknowledgement
        itself (e.g. ``{"t": "res", "r": 200, "opt": [...], "p": [...]}``)
        rather than the property staging of :meth:`push_state_update`.

        Args:
            opt: property names
            p: values, same order as ``opt``
            strict_ack: when True a missing reply raises ``asyncio.TimeoutError``;
                when False (default) it is logged and None is returned, since
                some modules execute commands without acknowledging them.
            timeout: seconds to wait for the reply; defaults to the device's
                command timeout when strict, 2 s otherwise. The gateway
                transport answers in-band and ignores this.

        Returns:
            The decrypted reply pack, or None when nothing came back and
            ``strict_ack`` is False.
        """
        opt, p = list(opt), list(p)
        if not self.beeper and BUZZER_FLAG not in opt:
            opt.append(BUZZER_FLAG)
            p.append(1)
        command = {'t': 'cmd', 'opt': opt, 'p': p}
        _LOGGER.debug(f"Sending command to {self.device_info.name}: {opt} = {p}")

        if not self.uses_mqtt:
            # The gateway answers in-band; a missing reply is a real failure.
            reply = await self._gateway_request(command)
            _LOGGER.debug(f"Command reply from {self.device_info.name}: {reply}")
            return reply

        if timeout is None:
            timeout = self._command_timeout if strict_ack else 2.0
        try:
            return await self._mqtt_request(command, timeout)
        except asyncio.TimeoutError:
            if strict_ack:
                raise
            _LOGGER.debug(f"No ack from {self.device_info.name} (command may still have been executed)")
            return None

    async def _mqtt_request(
        self, command: Dict[str, Any], timeout: float, addressing: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Publish *command* over MQTT and wait for the matching response.

        Returns the decrypted reply pack (``{"t": "dat", ...}`` for status,
        ``{"t": "res", ...}`` for an acknowledgement), or None when the reply
        could not be decrypted. Raises asyncio.TimeoutError when nothing comes
        back within *timeout*.

        Sub-devices are addressed per *addressing* (default: the device's
        current mode): ``tcid`` puts the unit id in the envelope, ``pack``
        puts the module in the envelope and the unit inside the pack.
        """
        addressing = addressing or self._addressing
        if self.is_sub_device and addressing == ADDRESSING_PACK:
            payload = {**command, 'mac': self._child_mac}
            tcid = None  # envelope addressed to the module
        else:
            payload = command
            tcid = self._child_mac

        self._response_event = asyncio.Event()
        self._response_data = None
        try:
            await self._mqtt_client.publish_command(
                self._parent_mac,
                payload,
                self.device_cipher,
                tcid
            )
            await asyncio.wait_for(self._response_event.wait(), timeout=timeout)
            return self._response_data
        finally:
            self._response_event = None
            self._response_data = None

    async def _gateway_status_cols(self, cols: List[str]) -> Dict[str, Any]:
        """Status over the gateway, in chunks when the module wants it that way.

        Old controllers on the line protocol accept a five-column request but
        drop the ~80-column one (#6). The first request that goes unanswered
        is retried with GATEWAY_PROBE_COLS; if that answers, the module is
        marked as needing chunked requests and the rest is fetched in chunks
        of GATEWAY_COLS_CHUNK. The limit is shared with sibling units.
        """
        # Siblings may have learned the limit while we were waiting.
        hinted = _MODULE_HINTS.get(self._parent_mac, {}).get("gateway_cols_limit")
        if hinted is not None and (self._gateway_cols_limit is None or hinted < self._gateway_cols_limit):
            self._gateway_cols_limit = hinted
        limit = self._gateway_cols_limit
        if limit is None:
            try:
                reply = await self._gateway_request({'t': 'status', 'cols': cols})
                return self._status_data(reply)
            except DeviceOfflineError:
                raise  # nobody to probe
            except asyncio.TimeoutError:
                if len(cols) <= len(GATEWAY_PROBE_COLS):
                    raise
                _LOGGER.debug(
                    f"{self.device_info.name}: no answer to a {len(cols)}-column status, "
                    f"trying {len(GATEWAY_PROBE_COLS)} columns"
                )
                reply = await self._gateway_request({'t': 'status', 'cols': GATEWAY_PROBE_COLS})
                data = self._status_data(reply)
                hinted = _MODULE_HINTS.get(self._parent_mac, {}).get("gateway_cols_limit")
                limit = min(GATEWAY_COLS_CHUNK, hinted) if hinted else GATEWAY_COLS_CHUNK
                self._gateway_cols_limit = limit
                self._remember(gateway_cols_limit=limit)
                _LOGGER.info(
                    f"{self.device_info.name}: module answers short status requests only; "
                    f"splitting into chunks of {limit} columns"
                )
                rest = [c for c in cols if c not in GATEWAY_PROBE_COLS]
            else:  # pragma: no cover - defensive
                rest = cols
        else:
            data, rest = {}, cols
        missing: List[str] = []
        i = 0
        while i < len(rest):
            chunk = rest[i:i + limit]
            try:
                reply = await self._gateway_request({'t': 'status', 'cols': chunk})
            except asyncio.TimeoutError:
                if limit > GATEWAY_MIN_CHUNK:
                    # Smaller pieces, same position; remember it for the siblings.
                    limit = max(GATEWAY_MIN_CHUNK, limit // 2)
                    self._gateway_cols_limit = limit
                    self._remember(gateway_cols_limit=limit)
                    _LOGGER.debug(
                        f"{self.device_info.name}: no answer to a {len(chunk)}-column chunk, "
                        f"continuing with chunks of {limit}"
                    )
                    continue
                # Already at the minimum: keep what we have rather than lose the unit.
                missing.extend(chunk)
                i += len(chunk)
                continue
            data.update(self._status_data(reply))
            i += len(chunk)
        if missing:
            if data:
                _LOGGER.warning(
                    f"{self.device_info.name}: {len(missing)} of {len(cols)} properties went "
                    f"unanswered this poll (e.g. {', '.join(missing[:5])}); keeping the rest"
                )
            else:
                raise asyncio.TimeoutError(f"{self.device_info.name}: no status chunk answered")
        return data

    def _gateway_variants(self) -> List[str]:
        """Addressing variants to try, the learned one first."""
        if not self.is_sub_device:
            return [GATEWAY_VIA_UNIT]
        if not self._gateway_variant:
            self._gateway_variant = _MODULE_HINTS.get(self._parent_mac, {}).get("gateway_variant")
        if self._gateway_variant:
            return [self._gateway_variant]
        return list(_GATEWAY_SUB_DEVICE_VARIANTS)

    async def _gateway_send(self, variant: str, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send *command* with one addressing variant, one at a time per module."""
        lock = _MODULE_GATEWAY_LOCKS.setdefault(self._parent_mac, asyncio.Lock())
        async with lock:
            last = _MODULE_GATEWAY_LAST.get(self._parent_mac)
            if last is not None:
                wait = GATEWAY_REQUEST_SPACING - (asyncio.get_running_loop().time() - last)
                if wait > 0:
                    await asyncio.sleep(wait)
            try:
                return await self._gateway_send_now(variant, command)
            finally:
                _MODULE_GATEWAY_LAST[self._parent_mac] = asyncio.get_running_loop().time()

    async def _gateway_send_now(self, variant: str, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send *command* with one addressing variant."""
        cipher = self._gateway_cipher()
        if variant == GATEWAY_VIA_UNIT:
            return await self._gateway.request(
                self._child_mac, cipher, {**command, "mac": self._child_mac}
            )
        if variant == GATEWAY_VIA_MODULE:
            return await self._gateway.request(
                self._parent_mac, cipher, {**command, "mac": self._child_mac}
            )
        if variant == GATEWAY_VIA_MODULE_AS_UNIT:
            return await self._gateway.request(
                self._parent_mac, cipher, {**command, "mac": self._child_mac}, tcid=self._child_mac
            )
        return await self._gateway.request(self._parent_mac, cipher, command)

    async def _gateway_request(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send *command* through the legacy gateway.

        A standalone module is addressed directly (its own gateway, its own
        MAC in the envelope and in the pack, as the Gree+ app does). A unit
        behind a module may be reachable through its own gateway entry, or
        only through the module's, named in the envelope or inside the pack
        (#6, #33, #44) -- the variants in _GATEWAY_SUB_DEVICE_VARIANTS are
        tried once and the one that answers is kept for this unit and its
        siblings. QueryOnline "offline" skips a variant without waiting.
        """
        variants = self._gateway_variants()
        last_error: Optional[BaseException] = None
        offline: set = set()
        for variant in variants:
            route = self._child_mac if variant == GATEWAY_VIA_UNIT else self._parent_mac
            if route in offline:
                continue
            try:
                reply = await self._gateway_send(variant, command)
            except DeviceOfflineError as err:
                last_error = err
                offline.add(route)
                _LOGGER.debug(f"{self.device_info.name}: gateway variant '{variant}': {route} offline for QueryOnline")
                continue
            except asyncio.TimeoutError as err:
                last_error = err
                _LOGGER.debug(f"{self.device_info.name}: gateway variant '{variant}': no answer")
                continue
            if self._gateway_variant != variant:
                self._gateway_variant = variant
                self._remember(gateway_variant=variant)
                if self.is_sub_device:
                    _LOGGER.info(
                        f"{self.device_info.name}: reached through the gateway as '{variant}' "
                        f"(module {self._parent_mac}, unit {self._child_mac}); keeping it"
                    )
            return reply
        assert last_error is not None
        raise last_error

    async def _gateway_status(self, command: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a status pack through the legacy gateway and decode it."""
        reply = await self._gateway_request(command)
        return self._decode_dat(reply)

    @staticmethod
    def _decode_dat(pack: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Turn a ``{"t":"dat","cols":[...],"dat":[...]}`` pack into a dict."""
        if not pack or pack.get('t') != 'dat':
            return None
        cols = pack.get('cols', [])
        dat = pack.get('dat', [])
        if cols and dat and len(cols) == len(dat):
            return dict(zip(cols, dat))
        return None
    
    async def push_state_update(self):
        """Push pending state updates to device
        
        IMPORTANT: Cloud devices require sequential command sending.
        Each command must wait for response before sending the next.
        """
        if not self._dirty:
            return
        
        _LOGGER.debug(f"Pushing state updates to cloud device: {self.device_info.name}")
        
        # Build commands with proper ordering (Mode first, Power last)
        commands = self._build_command_sequence()
        
        # Send commands sequentially
        for cmd in commands:
            await self._send_command(cmd['opt'], cmd['p'])
        
        self._dirty.clear()
    
    def _build_command_sequence(self) -> List[Dict[str, Any]]:
        """Build command sequence with proper ordering
        
        Order matters for cloud devices:
        1. Mode first (if present)
        2. Temperature with bit/unit
        3. Other properties
        4. Power last (if present)
        """
        commands = []
        remaining = {}
        
        # Collect pending updates
        for prop_name in self._dirty:
            value = self._properties.get(prop_name)
            remaining[prop_name] = value
        
        # Mode first
        if Props.MODE.value in remaining:
            commands.append({
                'opt': [Props.MODE.value],
                'p': [remaining[Props.MODE.value]]
            })
            del remaining[Props.MODE.value]
        
        # Temperature-related properties must always travel together in one
        # command: a 0.5C step where the whole-degree part is unchanged only
        # marks TEMP_BIT/TEMP_DECI/TEMP_HALF_DEGREE dirty, and the device
        # ignores those without SetTem present alongside them.
        temp_group = (
            Props.TEMP_SET.value,
            Props.TEMP_BIT.value,
            Props.TEMP_DECI.value,
            Props.TEMP_HALF_DEGREE.value,
        )
        if any(p in remaining for p in temp_group):
            temp_opt = []
            temp_p = []
            for p in temp_group:
                value = self._properties.get(p)
                if value is not None:
                    temp_opt.append(p)
                    temp_p.append(value)
                remaining.pop(p, None)

            if Props.TEMP_UNIT.value in remaining:
                temp_opt.append(Props.TEMP_UNIT.value)
                temp_p.append(remaining[Props.TEMP_UNIT.value])
                del remaining[Props.TEMP_UNIT.value]

            commands.append({'opt': temp_opt, 'p': temp_p})
        
        # Save Power for last
        has_power = Props.POWER.value in remaining
        power_value = remaining.pop(Props.POWER.value, None)
        
        # Other properties
        for key, value in remaining.items():
            commands.append({'opt': [key], 'p': [value]})
        
        # Power last
        if has_power:
            commands.append({
                'opt': [Props.POWER.value],
                'p': [power_value]
            })
        
        return commands
    
    async def _send_command(self, opt: List[str], p: List[Any]) -> None:
        """Send one command from push_state_update(); a missing ack is tolerated."""
        await self.send_command(opt, p)
    
    def _reply_is_for_other_unit(self, pack: Dict[str, Any]) -> bool:
        """True when a decrypted pack names a different unit behind this module.

        Several units behind one module share the module's topics, so every
        head receives every reply, and a pack carrying ``sub``/``mac`` for a
        sibling must be ignored or one head's state overwrites the others
        (#6, #33). Only ids of units actually instantiated behind the same
        module count as "someone else": some modules put an internal id in
        ``mac`` and the cloud MAC in ``cid`` (#66), and such packs are ours.
        A ``cid`` equal to our MAC settles it outright.
        """
        if pack.get('cid') in (self._child_mac, self._parent_mac):
            return False
        siblings = _UNITS_BY_MODULE.get(self._parent_mac, set()) - {self._child_mac}
        if not siblings:
            return False
        for key in _REPLY_UNIT_KEYS:
            unit = pack.get(key)
            if isinstance(unit, str) and unit in siblings:
                return True
        return False

    def _handle_mqtt_message(self, topic: str, message: MqttDeviceMessage) -> None:
        """Handle incoming MQTT messages"""
        # Check if message is for this device
        if self._parent_mac not in topic and self._child_mac not in topic:
            return

        is_response = 'response/' in topic
        is_status = 'status/' in topic
        if not (is_response or is_status):
            if 'connect/' in topic:
                _LOGGER.info(f"Device {self.device_info.name} connected to cloud")
            return

        decrypted: Optional[Dict[str, Any]] = None
        if message.pack:
            try:
                decrypted = self.device_cipher.decrypt(message.pack)
                _LOGGER.debug(f"{'Response' if is_response else 'Status'} decrypted: {decrypted}")
            except Exception as e:
                _LOGGER.debug(f"Could not decrypt {'response' if is_response else 'status'}: {e}")
            if isinstance(decrypted, dict) and self._reply_is_for_other_unit(decrypted):
                return

        if self._response_event:
            # Somebody is waiting: a response always completes the wait (a bare
            # ack carries no pack); a status push only when it decoded.
            if is_response or decrypted is not None:
                self._response_data = decrypted
                self._response_event.set()
            return

        # Unsolicited data (status push, or a late response)
        data = self._decode_dat(decrypted) if decrypted else None
        if data:
            self.handle_state_update(**data)
    
    def handle_state_update(self, **kwargs) -> None:
        """Normalise what commercial/VRF units report before applying it.

        Indoor units behind a module (#6, #33) differ from split units:
        unsupported numeric fields come back as ``""`` (applying those would
        clobber good values), numbers may arrive as strings, and the room
        temperature is reported as ``InTem`` rather than ``TemSen`` (same +40
        offset). Everything else passes through unchanged.
        """
        cleaned: Dict[str, Any] = {}
        if 'hid' in kwargs and not isinstance(kwargs['hid'], str):
            kwargs.pop('hid')  # some modules echo a number where the firmware string belongs
        for key, value in kwargs.items():
            if isinstance(value, str):
                text = value.strip()
                if text == "":
                    continue
                if key != 'hid':
                    try:
                        value = int(text)
                    except ValueError:
                        try:
                            value = float(text)
                        except ValueError:
                            pass
            cleaned[key] = value

        in_temp = cleaned.get('InTem')
        if isinstance(in_temp, (int, float)) and not isinstance(in_temp, bool):
            tem_sen = cleaned.get(Props.TEMP_SENSOR.value)
            if not isinstance(tem_sen, (int, float)) or tem_sen == 0:
                cleaned[Props.TEMP_SENSOR.value] = in_temp

        super().handle_state_update(**cleaned)

    async def close(self):
        """Close device and cleanup resources"""
        _UNITS_BY_MODULE.get(self._parent_mac, set()).discard(self._child_mac)
        try:
            if self._mqtt_client is not None:
                # Unsubscribe from MQTT topics
                if self.uses_mqtt:
                    await self._mqtt_client.unsubscribe_from_device(self._parent_mac)
                
                # Remove message handler
                self._mqtt_client.remove_message_handler(self._handle_mqtt_message)
            
            # Close transport if exists
            if self._transport:
                super().close()
        
        except Exception as e:
            _LOGGER.warning(f"Error closing cloud device: {e}")
    
    def __repr__(self) -> str:
        return (f"CloudDevice(name={self.device_info.name}, "
                f"mac={self.device_info.mac}, "
                f"parent_mac={self._parent_mac}, "
                f"child_mac={self._child_mac}, "
                f"addressing={self._addressing}, "
                f"gateway_variant={self._gateway_variant}, "
                f"transport={self.transport})")
