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
