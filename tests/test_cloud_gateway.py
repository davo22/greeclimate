"""Tests for the legacy (pre-MQTT) cloud gateway transport."""
import asyncio
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from greeclimate_davo22.cipher import CipherV1, CipherV2
from greeclimate_davo22.cloud_api import CloudDeviceServer, GreeCloudApi
from greeclimate_davo22.cloud_device import CloudDevice
from greeclimate_davo22.cloud_gateway import (
    GreeCloudGateway,
    parse_protocol_version,
    uses_mqtt,
)
from greeclimate_davo22.deviceinfo import DeviceInfo
from greeclimate_davo22.exceptions import DeviceOfflineError

KEY = "0123456789abcdef"
MAC = "9424b8f61a50"


@pytest.mark.parametrize(
    "version,expected",
    [
        ("V2.0.0", (2, 0, 0)),
        ("v3.5.1", (3, 5, 1)),
        ("V3.2", (3, 2)),
        ("V1.1.13", (1, 1, 13)),
        ("3.4.0-beta", (3, 4, 0)),
        ("", None),
        (None, None),
        ("unknown", None),
    ],
)
def test_parse_protocol_version(version, expected):
    assert parse_protocol_version(version) == expected


@pytest.mark.parametrize(
    "version,expected",
    [
        ("V2.0.0", False),
        ("V1.1.13", False),
        ("V3.1.9", False),
        ("V3.2", True),
        ("V3.2.0", True),
        ("V3.5.0", True),
        ("V4.0.0", True),
        (None, True),  # unknown -> keep the historical default
        ("garbage", True),
    ],
)
def test_uses_mqtt(version, expected):
    assert uses_mqtt(version) is expected


def make_api(user_id=4242):
    api = MagicMock(spec=GreeCloudApi)
    api.user_id = user_id
    api.query_online = AsyncMock()
    return api


def test_build_envelope_and_parse_reply_round_trip():
    api = make_api()
    gw = GreeCloudGateway(api)
    cipher = CipherV1(KEY.encode())
    command = {"t": "status", "cols": ["Pow", "Mod"]}

    line = gw.build_envelope(MAC, cipher, command)

    assert line.endswith(b"\n")
    outer = json.loads(line)
    assert outer["cid"] == "app"
    assert outer["i"] == 0
    assert outer["t"] == "pack"
    assert outer["tcid"] == MAC
    assert outer["uid"] == 4242
    assert "tag" not in outer  # ECB only, never GCM
    assert cipher.decrypt(outer["pack"]) == command

    # The reply uses the same envelope and the same key.
    reply_pack, _ = cipher.encrypt({"t": "dat", "cols": ["Pow"], "dat": [1]})
    reply = json.dumps({"t": "pack", "tcid": "app", "pack": reply_pack}).encode() + b"\n"
    assert GreeCloudGateway.parse_reply(reply, cipher) == {"t": "dat", "cols": ["Pow"], "dat": [1]}


class FakeGatewayServer:
    """Stand-in for the Gree iot gateway: one line in, one line out.

    Plain TCP by default; pass ``ssl_context`` to serve TLS with a
    self-signed certificate, the way the real gateway does.
    """

    def __init__(self, cipher: CipherV1, reply=None, close_without_reply=False, ssl_context=None):
        self.cipher = cipher
        self.reply = reply if reply is not None else {"t": "dat", "cols": ["Pow", "SetTem"], "dat": [1, 24]}
        self.close_without_reply = close_without_reply
        self.ssl_context = ssl_context
        self.received = []
        self.server = None

    async def handle(self, reader, writer):
        line = await reader.readline()
        self.received.append(json.loads(line))
        if not self.close_without_reply:
            pack, _ = self.cipher.encrypt(self.reply)
            writer.write(json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
            await writer.drain()
        writer.close()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0, ssl=self.ssl_context)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()


@pytest.mark.asyncio
async def test_request_talks_to_server_from_query_online():
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=False)}
        gw = GreeCloudGateway(api, timeout=2)

        reply = await gw.request(MAC, cipher, {"t": "status", "cols": ["Pow", "SetTem"]})

        assert reply == {"t": "dat", "cols": ["Pow", "SetTem"], "dat": [1, 24]}
        api.query_online.assert_awaited_once_with([MAC])
        sent = srv.received[0]
        assert sent["tcid"] == MAC
        assert cipher.decrypt(sent["pack"]) == {"t": "status", "cols": ["Pow", "SetTem"]}


@pytest.fixture
def self_signed_server_context(tmp_path):
    """TLS context with a throwaway self-signed cert, like Gree's private CA."""
    import shutil
    import ssl
    import subprocess

    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")
    key = tmp_path / "key.pem"
    crt = tmp_path / "crt.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=iot devices", "-keyout", str(key), "-out", str(crt)],
        check=True, capture_output=True,
    )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(crt), str(key))
    return ctx


@pytest.mark.asyncio
async def test_request_over_tls_with_untrusted_certificate(self_signed_server_context):
    """The real gateway presents a cert from Gree's own CA, which must be accepted."""
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher, ssl_context=self_signed_server_context) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=True)}
        gw = GreeCloudGateway(api, timeout=5)

        reply = await gw.request(MAC, cipher, {"t": "status", "cols": ["Pow"]})

        assert reply["t"] == "dat"
        assert srv.received[0]["tcid"] == MAC


@pytest.mark.asyncio
async def test_request_caches_gateway_lookup():
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=False)}
        gw = GreeCloudGateway(api, timeout=2)

        await gw.request(MAC, cipher, {"t": "status", "cols": ["Pow"]})
        await gw.request(MAC, cipher, {"t": "status", "cols": ["Pow"]})

        assert api.query_online.await_count == 1
        assert len(srv.received) == 2


@pytest.mark.asyncio
async def test_request_offline_device_raises_offline_error():
    api = make_api()
    api.query_online.return_value = {}
    gw = GreeCloudGateway(api, timeout=2)

    with pytest.raises(DeviceOfflineError):
        await gw.request(MAC, CipherV1(KEY.encode()), {"t": "status", "cols": ["Pow"]})

    # DeviceOfflineError must be a TimeoutError so existing handlers catch it.
    assert issubclass(DeviceOfflineError, asyncio.TimeoutError)


@pytest.mark.asyncio
async def test_request_no_reply_raises_timeout_and_forgets_server():
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher, close_without_reply=True) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=False)}
        gw = GreeCloudGateway(api, timeout=2)

        with pytest.raises(asyncio.TimeoutError):
            await gw.request(MAC, cipher, {"t": "status", "cols": ["Pow"]})

        # Next request must re-resolve the gateway.
        await gw.server_for(MAC)
        assert api.query_online.await_count == 2


@pytest.mark.asyncio
async def test_request_connect_failure_raises_timeout():
    api = make_api()
    # Nothing listens on this port.
    async with FakeGatewayServer(CipherV1(KEY.encode())) as srv:
        port = srv.port
    api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", port, ssl=False)}
    gw = GreeCloudGateway(api, timeout=2)

    with pytest.raises(asyncio.TimeoutError):
        await gw.request(MAC, CipherV1(KEY.encode()), {"t": "status", "cols": ["Pow"]})
    assert MAC not in gw._servers


# --------------------------------------------------------------- CloudDevice ---

def make_device(version, gateway=None, mqtt=None, **kwargs):
    info = DeviceInfo("0.0.0.0", 0, MAC, "test-device", version=version)
    return CloudDevice(mqtt, info, device_key=KEY, gateway=gateway, **kwargs)


def test_transport_auto_picks_gateway_for_old_protocol():
    gw = GreeCloudGateway(make_api())
    device = make_device("V2.0.0", gateway=gw, mqtt=MagicMock())
    assert device.transport == "gateway"
    assert device.uses_mqtt is False


def test_transport_auto_picks_mqtt_for_new_protocol():
    gw = GreeCloudGateway(make_api())
    device = make_device("V3.5.0", gateway=gw, mqtt=MagicMock())
    assert device.transport == "mqtt"


def test_transport_defaults_to_mqtt_without_gateway():
    device = make_device("V2.0.0", gateway=None, mqtt=MagicMock())
    assert device.transport == "mqtt"


def test_transport_explicit_override():
    gw = GreeCloudGateway(make_api())
    assert make_device("V3.5.0", gateway=gw, mqtt=MagicMock(), transport="gateway").transport == "gateway"
    assert make_device("V2.0.0", gateway=gw, mqtt=MagicMock(), transport="mqtt").transport == "mqtt"


def test_mqtt_transport_requires_client():
    with pytest.raises(ValueError):
        make_device("V3.5.0", gateway=None, mqtt=None)


def test_gateway_device_without_mqtt_client_is_fine():
    gw = GreeCloudGateway(make_api())
    device = make_device("V2.0.0", gateway=gw, mqtt=None)
    assert device.transport == "gateway"


def test_gateway_cipher_is_always_ecb():
    gw = GreeCloudGateway(make_api())
    device = make_device("V2.0.0", gateway=gw, mqtt=None, cipher_version=2)
    assert isinstance(device.device_cipher, CipherV2)
    assert isinstance(device._gateway_cipher(), CipherV1)
    assert device._gateway_cipher().key == KEY


@pytest.mark.asyncio
async def test_gateway_device_bind_and_update_state():
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher, reply={"t": "dat", "cols": ["Pow", "SetTem", "Mod"], "dat": [1, 23, 1]}) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=False)}
        gw = GreeCloudGateway(api, timeout=2)
        mqtt = MagicMock()
        mqtt.subscribe_to_device = AsyncMock()
        device = make_device("V2.0.0", gateway=gw, mqtt=mqtt)

        await device.bind()

        mqtt.subscribe_to_device.assert_not_awaited()
        assert device.raw_properties["Pow"] == 1
        assert device.raw_properties["SetTem"] == 23
        status = cipher.decrypt(srv.received[0]["pack"])
        assert status["t"] == "status"
        assert "Pow" in status["cols"]
        assert "hid" in status["cols"]


@pytest.mark.asyncio
async def test_gateway_device_push_state_sends_cmd():
    cipher = CipherV1(KEY.encode())
    async with FakeGatewayServer(cipher, reply={"t": "res", "opt": ["Pow"], "p": [1], "val": [1], "r": 200}) as srv:
        api = make_api()
        api.query_online.return_value = {MAC: CloudDeviceServer("127.0.0.1", srv.port, ssl=False)}
        gw = GreeCloudGateway(api, timeout=2)
        device = make_device("V2.0.0", gateway=gw, mqtt=None)

        device.power = True
        await device.push_state_update()

        cmd = cipher.decrypt(srv.received[0]["pack"])
        assert cmd == {"t": "cmd", "opt": ["Pow"], "p": [1]}
        assert device._dirty == []


@pytest.mark.asyncio
async def test_gateway_device_update_state_timeout_propagates():
    api = make_api()
    api.query_online.return_value = {}
    gw = GreeCloudGateway(api, timeout=1)
    device = make_device("V2.0.0", gateway=gw, mqtt=None)

    with pytest.raises(asyncio.TimeoutError):
        await device.update_state()


@pytest.mark.asyncio
async def test_mqtt_update_state_timeout_propagates():
    """Issue #41: a silent device must surface as a timeout, not a success."""
    mqtt = MagicMock()
    mqtt.publish_command = AsyncMock()
    device = make_device("V3.5.0", gateway=None, mqtt=mqtt, command_timeout=0.05)

    with pytest.raises(asyncio.TimeoutError):
        await device.update_state()

    mqtt.publish_command.assert_awaited_once()
    assert device._response_event is None


@pytest.mark.asyncio
async def test_mqtt_update_state_applies_response():
    mqtt = MagicMock()
    device = make_device("V3.5.0", gateway=None, mqtt=mqtt, command_timeout=1)
    cipher = CipherV1(KEY.encode())

    async def publish(parent_mac, command, dev_cipher, child_mac):
        pack, _ = cipher.encrypt({"t": "dat", "cols": ["Pow", "SetTem"], "dat": [1, 25]})
        msg = MagicMock()
        msg.pack = pack
        device._handle_mqtt_message(f"response/{parent_mac}/x", msg)

    mqtt.publish_command = AsyncMock(side_effect=publish)

    await device.update_state()

    assert device.raw_properties["SetTem"] == 25


@pytest.mark.asyncio
async def test_mqtt_send_command_without_ack_is_tolerated():
    mqtt = MagicMock()
    mqtt.publish_command = AsyncMock()
    device = make_device("V3.5.0", gateway=None, mqtt=mqtt)

    async def never_answers(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError

    with patch("greeclimate_davo22.cloud_device.asyncio.wait_for", side_effect=never_answers):
        await device._send_command(["Pow"], [1])  # must not raise


# ------------------------------------------------------------- cloud API ---

@pytest.mark.asyncio
async def test_query_online_signs_mac_list_and_parses_servers():
    api = GreeCloudApi("https://eugrih.gree.com", "u@example.com", "pw")
    api.user_id = 77
    api.token = "tok"
    captured = {}

    async def fake_send(endpoint, data):
        captured["endpoint"] = endpoint
        captured["body"] = json.loads(data)
        reply = {"r": 200, "online": [
            {"mac": "aabbccddeeff", "svr": "eu09.as.gree.com:16387", "isSSL": True},
            {"mac": "112233445566", "svr": "eu01.as.gree.com:16380"},
        ]}
        import base64
        return base64.b64encode(api._encrypt(json.dumps(reply))).decode()

    try:
        with patch.object(api, "_send_request", side_effect=fake_send):
            servers = await api.query_online(["aabbccddeeff", "112233445566", "000000000000"])
    finally:
        await api.close()

    assert captured["endpoint"] == "/App/QueryOnline"
    body = captured["body"]
    assert body["macs"] == ["aabbccddeeff", "112233445566", "000000000000"]
    expected = hashlib.md5(
        f"{GreeCloudApi.APP_HASH}_tok_77_aabbccddeeff_112233445566_000000000000".encode()
    ).hexdigest()
    assert body["datVc"] == expected

    assert servers == {
        "aabbccddeeff": CloudDeviceServer("eu09.as.gree.com", 16387, True),
        "112233445566": CloudDeviceServer("eu01.as.gree.com", 16380, False),
    }
