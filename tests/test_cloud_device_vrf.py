"""Multi-head (VRF) addressing, reply routing and the public command API.

Topology from davo22/homeassistant-gree-cloud#6: four indoor units behind one
module. Each unit record carries the module MAC as ``pmac``; unit ids do not
derive from the module MAC, so the old "strip 00" guess pointed at a topic
nobody listened on.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from greeclimate_davo22.cipher import CipherV1
from greeclimate_davo22.cloud_api import CloudDeviceInfo, GreeCloudApi
from greeclimate_davo22.cloud_device import (
    ADDRESSING_PACK,
    ADDRESSING_TCID,
    CloudDevice,
)
from greeclimate_davo22.deviceinfo import DeviceInfo

KEY = "0123456789abcdef"
MODULE = "9424b8b368bd"
HEAD_A = "7b65b424500000"
HEAD_B = "c3beaa24940000"


def make_head(mac, mqtt, parent=MODULE, **kwargs):
    info = DeviceInfo("0.0.0.0", 0, mac, f"head-{mac[:4]}", version="V1.1.0.0")
    return CloudDevice(mqtt, info, device_key=KEY, parent_mac=parent, command_timeout=0.2, **kwargs)


class FakeModule:
    """Stands in for the broker + module: records publishes, answers per policy."""

    def __init__(self, devices_by_unit):
        self.cipher = CipherV1(KEY.encode())
        self.published = []
        self.devices_by_unit = devices_by_unit  # unit mac -> device object(s) to notify
        self.answer_tcid = True   # answer requests whose envelope names the unit
        self.answer_pack = True   # answer requests that name the unit inside the pack
        self.reply_unit_key = "sub"
        self.reply_with_unit = True

    def publish_command(self):
        async def publish(parent_mac, command, cipher, tcid=None):
            unit = command.get("mac") or tcid
            via_pack = "mac" in command
            self.published.append({"topic": f"request/{parent_mac}", "tcid": tcid or parent_mac, "pack": command})
            if via_pack and not self.answer_pack:
                return
            if not via_pack and not self.answer_tcid:
                return
            if command["t"] == "status":
                reply = {"t": "dat", "cols": ["Pow", "SetTem", "InTem", "TemUn"], "dat": [1, 22, 61, ""]}
            else:
                reply = {"t": "res", "r": 200, "opt": command["opt"], "p": command["p"], "val": command["p"]}
            if self.reply_with_unit:
                reply[self.reply_unit_key] = unit
            pack, _ = cipher.encrypt(reply)
            msg = MagicMock()
            msg.pack = pack
            # Shared module topic: every head sees every reply.
            for devices in self.devices_by_unit.values():
                for device in devices:
                    device._handle_mqtt_message(f"response/{parent_mac}/app", msg)
        return publish


def make_system(**module_kwargs):
    mqtt = MagicMock()
    module = FakeModule({})
    for k, v in module_kwargs.items():
        setattr(module, k, v)
    mqtt.publish_command = AsyncMock(side_effect=module.publish_command())
    mqtt.subscribe_to_device = AsyncMock()
    head_a = make_head(HEAD_A, mqtt)
    head_b = make_head(HEAD_B, mqtt)
    module.devices_by_unit = {HEAD_A: [head_a], HEAD_B: [head_b]}
    return mqtt, module, head_a, head_b


def test_parent_comes_from_pmac_not_from_string_surgery():
    head = make_head(HEAD_A, MagicMock())
    assert head._parent_mac == MODULE
    assert head._child_mac == HEAD_A
    assert head.is_sub_device
    assert head.addressing == ADDRESSING_TCID


def test_parent_falls_back_to_strip_00_without_pmac():
    info = DeviceInfo("0.0.0.0", 0, "c03937a616ab00", "single-head")
    device = CloudDevice(MagicMock(), info, device_key=KEY)
    assert device._parent_mac == "c03937a616ab"
    assert device.is_sub_device


def test_standalone_module_is_not_a_sub_device():
    info = DeviceInfo("0.0.0.0", 0, "c03937a426d8", "split")
    device = CloudDevice(MagicMock(), info, device_key=KEY, parent_mac="")
    assert device._parent_mac == "c03937a426d8"
    assert not device.is_sub_device


@pytest.mark.asyncio
async def test_heads_publish_to_module_topic_with_unit_as_tcid():
    mqtt, module, head_a, head_b = make_system()

    await head_a.bind()
    await head_b.bind()

    mqtt.subscribe_to_device.assert_any_await(MODULE)
    assert all(p["topic"] == f"request/{MODULE}" for p in module.published)
    assert [p["tcid"] for p in module.published] == [HEAD_A, HEAD_B]
    assert all("mac" not in p["pack"] for p in module.published)
    assert head_a.raw_properties["SetTem"] == 22


@pytest.mark.asyncio
async def test_reply_for_other_head_is_ignored():
    """Both heads see every reply on the shared topic; only the named one applies it."""
    mqtt, module, head_a, head_b = make_system()
    cipher = module.cipher

    pack, _ = cipher.encrypt({"t": "dat", "sub": HEAD_B, "cols": ["SetTem"], "dat": [18]})
    msg = MagicMock(); msg.pack = pack
    head_a._handle_mqtt_message(f"response/{MODULE}/app", msg)
    head_b._handle_mqtt_message(f"status/{MODULE}/app", msg)

    assert "SetTem" not in head_a.raw_properties
    assert head_b.raw_properties["SetTem"] == 18


@pytest.mark.asyncio
async def test_reply_for_other_head_does_not_complete_a_pending_request():
    mqtt, module, head_a, head_b = make_system()
    cipher = module.cipher

    async def publish(parent_mac, command, dev_cipher, tcid=None):
        # Wrong unit answers first, right one afterwards.
        for unit, value in ((HEAD_B, 99), (HEAD_A, 22)):
            pack, _ = cipher.encrypt({"t": "dat", "mac": unit, "cols": ["SetTem"], "dat": [value]})
            msg = MagicMock(); msg.pack = pack
            head_a._handle_mqtt_message(f"response/{parent_mac}/app", msg)

    mqtt.publish_command = AsyncMock(side_effect=publish)
    data = await head_a.request_status(["SetTem"])
    assert data == {"SetTem": 22}


@pytest.mark.asyncio
async def test_falls_back_to_pack_addressing_when_tcid_gets_no_answer():
    """Issue #33 systems only answer when the unit is named inside the pack."""
    mqtt, module, head_a, _ = make_system(answer_tcid=False)

    await head_a.update_state()

    assert head_a.addressing == ADDRESSING_PACK
    assert len(module.published) == 2
    first, second = module.published
    assert first["tcid"] == HEAD_A and "mac" not in first["pack"]
    assert second["tcid"] == MODULE and second["pack"]["mac"] == HEAD_A
    assert head_a.raw_properties["SetTem"] == 22

    # Commands follow the addressing that worked.
    module.published.clear()
    head_a.power = False
    await head_a.push_state_update()
    assert module.published[0]["tcid"] == MODULE
    assert module.published[0]["pack"] == {"t": "cmd", "opt": ["Pow"], "p": [0], "mac": HEAD_A}


@pytest.mark.asyncio
async def test_no_fallback_for_standalone_device():
    mqtt = MagicMock()
    mqtt.publish_command = AsyncMock()
    info = DeviceInfo("0.0.0.0", 0, "c03937a426d8", "split")
    device = CloudDevice(mqtt, info, device_key=KEY, command_timeout=0.05)

    with pytest.raises(asyncio.TimeoutError):
        await device.update_state()
    mqtt.publish_command.assert_awaited_once()


@pytest.mark.asyncio
async def test_silent_sub_device_raises_after_both_addressings():
    mqtt, module, head_a, _ = make_system(answer_tcid=False, answer_pack=False)

    with pytest.raises(asyncio.TimeoutError):
        await head_a.update_state()
    assert [p["tcid"] for p in module.published] == [HEAD_A, MODULE]
    assert head_a.addressing == ADDRESSING_TCID


@pytest.mark.asyncio
async def test_vrf_values_are_normalised():
    """InTem stands in for TemSen, "" is dropped, numeric strings become numbers."""
    mqtt, module, head_a, _ = make_system()
    head_a.handle_state_update(TemUn=0, SetTem=24)

    head_a.handle_state_update(InTem="61", TemUn="", SetTem="25", Pow=1, hid="362001+U-X.bin")

    assert head_a.raw_properties["TemSen"] == 61
    assert head_a.raw_properties["TemUn"] == 0        # "" did not clobber it
    assert head_a.raw_properties["SetTem"] == 25
    assert head_a.hid == "362001+U-X.bin"


def test_reported_temsen_wins_over_intem():
    head = make_head(HEAD_A, MagicMock())
    head.handle_state_update(TemSen=62, InTem=58)
    assert head.raw_properties["TemSen"] == 62


# ------------------------------------------------------------ public API ---

@pytest.mark.asyncio
async def test_send_command_returns_decrypted_ack():
    mqtt, module, head_a, _ = make_system()

    reply = await head_a.send_command(["Pow", "SetTem"], [1, 23], strict_ack=True)

    assert reply["t"] == "res" and reply["r"] == 200
    assert reply["opt"] == ["Pow", "SetTem"] and reply["p"] == [1, 23]
    assert module.published[-1]["pack"] == {"t": "cmd", "opt": ["Pow", "SetTem"], "p": [1, 23]}


@pytest.mark.asyncio
async def test_send_command_strict_raises_without_ack():
    mqtt, module, head_a, _ = make_system(answer_tcid=False)
    with pytest.raises(asyncio.TimeoutError):
        await head_a.send_command(["Pow"], [1], strict_ack=True, timeout=0.05)


@pytest.mark.asyncio
async def test_send_command_lenient_returns_none_without_ack():
    mqtt, module, head_a, _ = make_system(answer_tcid=False)
    assert await head_a.send_command(["Pow"], [1], timeout=0.05) is None


@pytest.mark.asyncio
async def test_request_status_returns_columns_without_touching_state():
    mqtt, module, head_a, _ = make_system()

    data = await head_a.request_status(["Pow", "SetTem"])

    assert data["SetTem"] == 22
    assert "SetTem" not in head_a.raw_properties


# ------------------------------------------------------ duplicate filter ---

@pytest.mark.asyncio
async def test_filter_drops_module_record_named_as_pmac():
    api = GreeCloudApi("https://augrih.gree.com", "u@example.com", "pw")
    heads = [
        CloudDeviceInfo(name=n, mac=m, key="k", parent_mac=MODULE, mid="6000")
        for n, m in (("Living Room", HEAD_A), ("Bedroom", HEAD_B), ("Den", "c14d9403500000"), ("Study", "49ad0b24890000"))
    ]
    module = CloudDeviceInfo(name="Commercial AC", mac=MODULE, key="k", mid="60")

    kept = api._filter_duplicate_devices(heads + [module])
    await api.close()

    assert [d.mac for d in kept] == [d.mac for d in heads]


@pytest.mark.asyncio
async def test_filter_single_head_module_keeps_unit_drops_module():
    """Dawid's own commercial module: unit c03937a616ab00 with pmac c03937a616ab."""
    api = GreeCloudApi("https://eugrih.gree.com", "u@example.com", "pw")
    module = CloudDeviceInfo(name="Commercial AC", mac="c03937a616ab", key="k", mid="60")
    unit = CloudDeviceInfo(name="Commercial ACab00", mac="c03937a616ab00", key="k", parent_mac="c03937a616ab", mid="6400")
    split = CloudDeviceInfo(name="AC26d8", mac="c03937a426d8", key="k2", mid="10001")

    kept = api._filter_duplicate_devices([split, module, unit])
    await api.close()

    assert [d.mac for d in kept] == ["c03937a426d8", "c03937a616ab00"]


# ------------------------------------------------------------------ beeper ---

@pytest.mark.asyncio
async def test_beeper_off_appends_buzzer_flag_to_every_command():
    """davo22/homeassistant-gree-cloud#50: Buzzer_ON_OFF=1 on each cmd pack keeps the unit silent."""
    mqtt, module, head_a, _ = make_system()
    head_a.beeper = False

    head_a.mode = 1
    head_a.power = True
    await head_a.push_state_update()

    cmds = [p["pack"] for p in module.published if p["pack"]["t"] == "cmd"]
    assert len(cmds) == 2
    for cmd in cmds:
        assert cmd["opt"][-1] == "Buzzer_ON_OFF" and cmd["p"][-1] == 1
    assert cmds[0]["opt"][0] == "Mod" and cmds[-1]["opt"][0] == "Pow"


@pytest.mark.asyncio
async def test_beeper_on_sends_plain_commands_and_status_never_carries_flag():
    mqtt, module, head_a, _ = make_system()
    head_a.beeper = False
    await head_a.request_status(["Pow"])
    head_a.beeper = True
    await head_a.send_command(["Pow"], [1])

    assert all("Buzzer_ON_OFF" not in p["pack"].get("opt", []) for p in module.published)
    assert "Buzzer_ON_OFF" not in module.published[0]["pack"]


@pytest.mark.asyncio
async def test_beeper_flag_not_duplicated_when_caller_sets_it():
    mqtt, module, head_a, _ = make_system()
    head_a.beeper = False
    await head_a.send_command(["Pow", "Buzzer_ON_OFF"], [1, 0])
    cmd = module.published[-1]["pack"]
    assert cmd["opt"].count("Buzzer_ON_OFF") == 1 and cmd["p"] == [1, 0]


# ------------------------------------------------------- transport fallback ---

from greeclimate_davo22.cloud_gateway import GreeCloudGateway  # noqa: E402


def make_gateway(offline=True):
    api = MagicMock()
    api.user_id = 1
    api.query_online = AsyncMock(return_value={})  # everything offline -> DeviceOfflineError
    gw = GreeCloudGateway(api, timeout=0.1)
    return gw




def test_standalone_old_module_still_starts_on_gateway():
    info = DeviceInfo("0.0.0.0", 0, "c03937a426d8", "split", version="V2.0.0")
    device = CloudDevice(MagicMock(), info, device_key=KEY, gateway=make_gateway(), command_timeout=0.05)
    assert device.transport == "gateway"


@pytest.mark.asyncio
async def test_bind_falls_back_to_gateway_when_mqtt_is_silent_and_back_when_both_are():
    mqtt = MagicMock()
    mqtt.publish_command = AsyncMock()
    mqtt.subscribe_to_device = AsyncMock()
    mqtt.unsubscribe_from_device = AsyncMock()
    head = make_head(HEAD_A, mqtt, gateway=make_gateway())  # gateway: offline -> TimeoutError subclass

    assert head.transport == "gateway"
    with pytest.raises(asyncio.TimeoutError):
        await head.bind()

    # gateway first (offline), then MQTT (both addressings), then restored gateway
    assert mqtt.publish_command.await_count == 2
    assert head.transport == "gateway"
    mqtt.unsubscribe_from_device.assert_awaited_once()


@pytest.mark.asyncio
async def test_bind_keeps_mqtt_after_gateway_answers():
    """Standalone V2.0.0 module silent on the gateway but alive on MQTT."""
    mqtt = MagicMock()
    mqtt.subscribe_to_device = AsyncMock()
    cipher = CipherV1(KEY.encode())
    info = DeviceInfo("0.0.0.0", 0, "c03937a426d8", "split", version="V2.0.0")
    device = CloudDevice(mqtt, info, device_key=KEY, gateway=make_gateway(), command_timeout=0.2)
    assert device.transport == "gateway"

    async def publish(parent_mac, command, dev_cipher, tcid=None):
        pack, _ = cipher.encrypt({"t": "dat", "cols": ["Pow"], "dat": [1]})
        msg = MagicMock(); msg.pack = pack
        device._handle_mqtt_message(f"response/{parent_mac}/app", msg)
    mqtt.publish_command = AsyncMock(side_effect=publish)

    await device.bind()

    assert device.transport == "mqtt"
    assert device.raw_properties["Pow"] == 1
    mqtt.subscribe_to_device.assert_awaited_once_with("c03937a426d8")


@pytest.mark.asyncio
async def test_non_dat_status_reply_is_logged_not_swallowed(caplog):
    """#6: 'Study' bound with an empty state because the module answered a
    status request with something that was not a dat pack."""
    import logging
    mqtt = MagicMock()
    head = make_head(HEAD_A, mqtt)
    cipher = CipherV1(KEY.encode())

    async def publish(parent_mac, command, dev_cipher, tcid=None):
        pack, _ = cipher.encrypt({"t": "res", "r": 404, "msg": "no such unit"})
        msg = MagicMock(); msg.pack = pack
        head._handle_mqtt_message(f"response/{parent_mac}/app", msg)
    mqtt.publish_command = AsyncMock(side_effect=publish)

    with caplog.at_level(logging.WARNING, logger="greeclimate_davo22.cloud_device"):
        data = await head.request_status(["Pow"])
    assert data == {}
    assert "answered with a 'res' pack instead of data" in caplog.text
    assert "404" in caplog.text


# ---------------------------------------------- gateway variants / chunks ---

import json as _json  # noqa: E402
from greeclimate_davo22.cloud_api import CloudDeviceServer  # noqa: E402
from greeclimate_davo22 import cloud_device as _cd  # noqa: E402


class PickyModuleGateway:
    """A commercial controller on the line protocol: answers only when the
    envelope names the unit (tcid) and the pack has at most 16 columns."""

    def __init__(self, cipher, max_cols=16, want_tcid="unit"):
        self.cipher = cipher
        self.max_cols = max_cols
        self.want_tcid = want_tcid
        self.received = []
        self.server = None

    async def handle(self, reader, writer):
        line = await reader.readline()
        outer = _json.loads(line)
        inner = self.cipher.decrypt(outer["pack"])
        self.received.append((outer["tcid"], inner))
        unit = inner.get("mac")
        ok_tcid = (outer["tcid"] == unit) if self.want_tcid == "unit" else (outer["tcid"] != unit)
        if inner.get("t") == "status" and ok_tcid and unit and len(inner["cols"]) <= self.max_cols:
            reply = {"t": "dat", "sub": unit, "cols": inner["cols"], "dat": [("" if c == "hid" else 1) for c in inner["cols"]]}
            pack, _ = self.cipher.encrypt(reply)
            writer.write(_json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
            await writer.drain()
        elif inner.get("t") == "cmd" and ok_tcid and unit:
            pack, _ = self.cipher.encrypt({"t": "res", "r": 200, "sub": unit, "opt": inner["opt"], "p": inner["p"]})
            writer.write(_json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
            await writer.drain()
        # otherwise: stay silent, like the real thing
        writer.close()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()


def _module_only_api(port):
    api = MagicMock(); api.user_id = 1
    api.query_online = AsyncMock(side_effect=lambda macs: {MODULE: CloudDeviceServer("127.0.0.1", port, ssl=False)} if macs == [MODULE] else {})
    return api


@pytest.mark.asyncio
async def test_sub_device_finds_the_variant_and_chunks_columns_then_siblings_skip_the_search():
    cipher = CipherV1(KEY.encode())
    async with PickyModuleGateway(cipher) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        head_a = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_A, "A", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        cols = [f"C{i}" for i in range(40)]

        data = await head_a.request_status(cols)

        assert set(cols) <= set(data)  # plus the five probe columns
        assert head_a._gateway_variant == "module-as-unit"
        assert head_a._gateway_cols_limit == 5
        # first contact: 5-col probe through the variants (module silent, module-as-unit
        # answers); then the 35 remaining columns in one go (silent: too long) and in
        # 7 chunks of 5
        tcids = [t for t, _ in srv.received]
        assert tcids[-3:] == [HEAD_A, HEAD_A, HEAD_A]
        answered = [inner for t, inner in srv.received if t == HEAD_A and len(inner["cols"]) <= 16]
        assert len(answered) == 9  # probe + 40 columns in chunks of 5 (the probe names are not in cols)
        assert _cd._MODULE_HINTS[MODULE] == {"gateway_variant": "module-as-unit", "gateway_cols_limit": 5}

        # A sibling created afterwards uses the learned variant and limit straight away.
        srv.received.clear()
        head_b = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_B, "B", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        data_b = await head_b.request_status(cols)
        assert set(cols) <= set(data_b)  # plus the probe columns
        assert all(t == HEAD_B and len(inner["cols"]) <= 16 for t, inner in srv.received)
        assert len(srv.received) == 1 + 7  # first-contact probe, then 35 columns in chunks of 5

        # Commands follow the learned variant.
        srv.received.clear()
        head_b.power = True
        await head_b.push_state_update()
        assert srv.received[0][0] == HEAD_B and srv.received[0][1]["mac"] == HEAD_B


@pytest.mark.asyncio
async def test_bind_remembers_transport_for_siblings():
    """Once head A ends up on the gateway, head B starts there instead of MQTT."""
    cipher = CipherV1(KEY.encode())
    async with PickyModuleGateway(cipher) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        mqtt = MagicMock(); mqtt.publish_command = AsyncMock(); mqtt.subscribe_to_device = AsyncMock(); mqtt.unsubscribe_from_device = AsyncMock()
        head_a = CloudDevice(mqtt, DeviceInfo("0.0.0.0", 0, HEAD_A, "A", version="V3.5.M"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, command_timeout=0.1)
        assert head_a.transport == "mqtt"
        await head_a.bind()
        assert head_a.transport == "gateway"
        assert _cd._MODULE_HINTS[MODULE]["transport"] == "gateway"

        head_b = CloudDevice(mqtt, DeviceInfo("0.0.0.0", 0, HEAD_B, "B", version="V3.5.M"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, command_timeout=0.1)
        assert head_b.transport == "gateway"
        published = mqtt.publish_command.await_count
        await head_b.bind()
        assert mqtt.publish_command.await_count == published  # no MQTT attempt at all
        assert head_b.raw_properties.get("Pow") == 1


# --------------------------------------------------------- #66 regression ---

def _pack_msg(cipher, payload):
    pack, _ = cipher.encrypt(payload)
    msg = MagicMock(); msg.pack = pack
    return msg


def test_internal_mac_with_own_cid_is_accepted_on_a_standalone_unit():
    """#66: U-WB05WR11 replies carry mac=internal id and cid=cloud MAC; 2.5.0 dropped them."""
    mqtt = MagicMock()
    unit = CloudDevice(mqtt, DeviceInfo("0.0.0.0", 0, "7cb8e6441234", "Salon", version="V3.2.M"), device_key=KEY)
    cipher = CipherV1(KEY.encode())
    unit._handle_mqtt_message("response/7cb8e6441234/app", _pack_msg(cipher, {
        "t": "dat", "uid": 1, "mac": "000000221859", "cid": "7cb8e6441234",
        "cols": ["Pow", "Mod"], "dat": [1, 4], "r": 200}))
    assert unit.raw_properties["Pow"] == 1


def test_unknown_mac_without_cid_is_accepted_when_there_are_no_siblings():
    mqtt = MagicMock()
    unit = CloudDevice(mqtt, DeviceInfo("0.0.0.0", 0, "7cb8e6441234", "Salon"), device_key=KEY)
    cipher = CipherV1(KEY.encode())
    unit._handle_mqtt_message("response/7cb8e6441234/app", _pack_msg(cipher, {
        "t": "dat", "mac": "deadbeef0000", "cols": ["Pow"], "dat": [1]}))
    assert unit.raw_properties["Pow"] == 1


def test_only_a_registered_sibling_id_is_treated_as_someone_else():
    mqtt, module, head_a, head_b = make_system()
    cipher = module.cipher
    # internal id, unknown to anyone -> accepted
    head_a._handle_mqtt_message(f"response/{MODULE}/app", _pack_msg(cipher, {
        "t": "dat", "mac": "000000221859", "cols": ["SetTem"], "dat": [21]}))
    assert head_a.raw_properties["SetTem"] == 21
    # the sibling's id -> dropped
    head_a._handle_mqtt_message(f"response/{MODULE}/app", _pack_msg(cipher, {
        "t": "dat", "sub": HEAD_B, "cols": ["SetTem"], "dat": [18]}))
    assert head_a.raw_properties["SetTem"] == 21
    # own cid wins even if mac names the sibling (defensive)
    head_a._handle_mqtt_message(f"response/{MODULE}/app", _pack_msg(cipher, {
        "t": "dat", "mac": HEAD_B, "cid": HEAD_A, "cols": ["SetTem"], "dat": [19]}))
    assert head_a.raw_properties["SetTem"] == 19


@pytest.mark.asyncio
async def test_closed_unit_is_no_longer_a_sibling():
    mqtt, module, head_a, head_b = make_system()
    mqtt.unsubscribe_from_device = AsyncMock(); mqtt.remove_message_handler = MagicMock()
    await head_b.close()
    cipher = module.cipher
    head_a._handle_mqtt_message(f"response/{MODULE}/app", _pack_msg(cipher, {
        "t": "dat", "sub": HEAD_B, "cols": ["SetTem"], "dat": [18]}))
    assert head_a.raw_properties["SetTem"] == 18


# ---------------------------------------------- gateway serialisation / chunks ---

class FlakyModuleGateway(PickyModuleGateway):
    """Answers plain module requests (tcid module, no mac) of <= max_cols columns,
    refuses to answer while another connection is open, and can fail a given
    chunk size once."""

    def __init__(self, cipher, max_cols=16, fail_sizes_once=()):
        super().__init__(cipher, max_cols=max_cols, want_tcid="module")
        self.open = 0
        self.max_open = 0
        self.fail_once = set(fail_sizes_once)

    async def handle(self, reader, writer):
        self.open += 1
        self.max_open = max(self.max_open, self.open)
        try:
            line = await reader.readline()
            outer = _json.loads(line)
            inner = self.cipher.decrypt(outer["pack"])
            self.received.append((outer["tcid"], inner))
            n = len(inner.get("cols", []))
            if self.open > 1 or inner.get("t") != "status" or "mac" in inner or n > self.max_cols:
                return
            if n in self.fail_once:
                self.fail_once.discard(n)
                return
            reply = {"t": "dat", "cols": inner["cols"], "dat": [("" if c == "hid" else 7) for c in inner["cols"]]}
            pack, _ = self.cipher.encrypt(reply)
            writer.write(_json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
            await writer.drain()
        finally:
            self.open -= 1
            writer.close()


@pytest.mark.asyncio
async def test_concurrent_heads_are_serialised_per_module_and_chunks_shrink_on_failure():
    cipher = CipherV1(KEY.encode())
    async with FlakyModuleGateway(cipher, max_cols=16, fail_sizes_once={16}) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        _cd.GATEWAY_REQUEST_SPACING = 0.01
        _cd.GATEWAY_COLS_CHUNK = 16  # a module that takes bigger chunks; restored by the fixture
        heads = [CloudDevice(None, DeviceInfo("0.0.0.0", 0, m, m[:4], version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
                 for m in (HEAD_A, HEAD_B, "c14d9403500000", "49ad0b24890000")]
        cols = [f"C{i}" for i in range(40)]

        results = await asyncio.gather(*(h.request_status(cols) for h in heads))

        assert srv.max_open == 1  # never two connections to the module at once
        for data in results:
            assert set(cols) <= set(data)
        # the 16-column chunk failed once -> halved to 8 and remembered
        assert _cd._MODULE_HINTS[MODULE]["gateway_cols_limit"] == 8
        assert _cd._MODULE_HINTS[MODULE]["gateway_variant"] == "module-plain"


@pytest.mark.asyncio
async def test_partial_answer_keeps_the_unit_alive(caplog):
    """A chunk that stays silent even at the minimum size is skipped, not fatal."""
    import logging
    cipher = CipherV1(KEY.encode())
    async with FlakyModuleGateway(cipher, max_cols=16, fail_sizes_once={5}) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        _cd.GATEWAY_REQUEST_SPACING = 0.01
        head = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_A, "A", version="V1.1.0.0"),
                           device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        head._gateway_cols_limit = 5
        head._gateway_warm = True  # a unit that has answered before; one chunk now stays silent
        cols = [f"C{i}" for i in range(12)]
        with caplog.at_level(logging.WARNING, logger="greeclimate_davo22.cloud_device"):
            data = await head.request_status(cols)
        assert 0 < len(data) < len(cols)
        assert "went unanswered this poll" in caplog.text


# ---------------------------------------------------- first contact probe ---

class ColdStartModuleGateway(PickyModuleGateway):
    """Answers a unit's requests of <= 16 columns only after that unit has
    sent a <= 5-column request first (what #6's controller does)."""

    def __init__(self, cipher):
        super().__init__(cipher, max_cols=16, want_tcid="module")
        self.warm = set()

    async def handle(self, reader, writer):
        line = await reader.readline()
        outer = _json.loads(line)
        inner = self.cipher.decrypt(outer["pack"])
        self.received.append((outer["tcid"], inner))
        unit = inner.get("mac")
        n = len(inner.get("cols", []))
        ok = inner.get("t") == "status" and unit and outer["tcid"] == MODULE and (
            n <= 5 or (unit in self.warm and n <= 16))
        if ok:
            self.warm.add(unit)
            reply = {"t": "dat", "r": 200, "mac": unit, "cols": inner["cols"],
                     "dat": [("" if c == "hid" else 3) for c in inner["cols"]]}
            pack, _ = self.cipher.encrypt(reply)
            writer.write(_json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
            await writer.drain()
        writer.close()


@pytest.mark.asyncio
async def test_first_head_probes_first_and_siblings_probe_before_chunks():
    cipher = CipherV1(KEY.encode())
    async with ColdStartModuleGateway(cipher) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        _cd.GATEWAY_REQUEST_SPACING = 0.01
        cols = [f"C{i}" for i in range(40)]
        head_a = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_A, "A", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        data = await head_a.request_status(cols)
        assert set(cols) <= set(data)
        # the very first thing sent for the unit was the 5-column probe
        first_for_a = next(inner for t, inner in srv.received if inner.get("mac") == HEAD_A)
        assert len(first_for_a["cols"]) == 5

        srv.received.clear()
        head_b = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_B, "B", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        assert head_b._gateway_cols_limit == 5 and head_b._gateway_variant == "module"
        data_b = await head_b.request_status(cols)
        assert set(cols) <= set(data_b)
        sizes = [len(inner["cols"]) for t, inner in srv.received]
        assert sizes[0] == 5 and all(n <= 16 for n in sizes)
        assert len(srv.received) == 1 + 8  # first-contact probe, then the 40 columns in chunks of 5


@pytest.mark.asyncio
async def test_no_cross_transport_fallback_when_module_transport_is_known():
    mqtt = MagicMock(); mqtt.publish_command = AsyncMock(); mqtt.subscribe_to_device = AsyncMock(); mqtt.unsubscribe_from_device = AsyncMock()
    api = MagicMock(); api.user_id = 1; api.query_online = AsyncMock(return_value={})
    gw = GreeCloudGateway(api, timeout=0.1)
    _cd._MODULE_HINTS[MODULE] = {"transport": "gateway", "gateway_variant": "module", "gateway_cols_limit": 16}
    head = CloudDevice(mqtt, DeviceInfo("0.0.0.0", 0, HEAD_B, "B", version="V1.1.0.0"),
                       device_key=KEY, gateway=gw, parent_mac=MODULE, command_timeout=0.1)
    assert head.transport == "gateway"
    with pytest.raises(asyncio.TimeoutError):
        await head.bind()
    mqtt.publish_command.assert_not_awaited()


# ------------------------------------------------- offline backoff / order ---

class OfflineModuleGateway(PickyModuleGateway):
    """Answers every request with the gateway's 'device is not online' pack."""

    async def handle(self, reader, writer):
        line = await reader.readline()
        outer = _json.loads(line)
        self.received.append((outer["tcid"], self.cipher.decrypt(outer["pack"])))
        pack, _ = self.cipher.encrypt({"t": "res", "r": 404, "msg": "device is not online"})
        writer.write(_json.dumps({"t": "pack", "pack": pack, "tcid": "app"}).encode() + b"\n")
        await writer.drain()
        writer.close()


@pytest.mark.asyncio
async def test_not_online_reply_backs_off_for_the_whole_module(caplog):
    import logging
    cipher = CipherV1(KEY.encode())
    async with OfflineModuleGateway(cipher) as srv:
        gw = GreeCloudGateway(_module_only_api(srv.port), timeout=0.3)
        _cd.GATEWAY_REQUEST_SPACING = 0.01
        head_a = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_A, "A", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        head_b = CloudDevice(None, DeviceInfo("0.0.0.0", 0, HEAD_B, "B", version="V1.1.0.0"),
                             device_key=KEY, gateway=gw, parent_mac=MODULE, transport="gateway")
        with caplog.at_level(logging.WARNING, logger="greeclimate_davo22.cloud_device"):
            with pytest.raises(asyncio.TimeoutError):
                await head_a.request_status(["Pow"])
        sent = len(srv.received)
        assert sent >= 1 and "reports module" in caplog.text
        # the sibling fails fast without touching the gateway
        with pytest.raises(asyncio.TimeoutError):
            await head_b.request_status(["Pow"])
        assert len(srv.received) == sent
        assert MODULE in _cd._MODULE_GATEWAY_OFFLINE_UNTIL


def test_old_sub_device_starts_on_the_gateway_again():
    mqtt = MagicMock()
    head = make_head(HEAD_A, mqtt, gateway=make_gateway())
    assert head.transport == "gateway"


def test_chunk_default_is_probe_size():
    assert _cd.GATEWAY_COLS_CHUNK == 5
