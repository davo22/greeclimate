"""Tests for CloudDevice, in particular its command-batching logic."""
from unittest.mock import MagicMock

from greeclimate.cloud_device import CloudDevice
from greeclimate.device import Props, TemperatureUnits
from greeclimate.deviceinfo import DeviceInfo


def make_cloud_device():
    device_info = DeviceInfo("0.0.0.0", 0, "f4911e7aca59", "test-device")
    return CloudDevice(MagicMock(), device_info, device_key="0123456789abcdef")


def test_build_command_sequence_bundles_temp_set_and_bit():
    """SetTem and TemRec must always be sent together, since the device
    ignores a lone TemRec update with no SetTem to apply it to."""
    device = make_cloud_device()
    device.set_property(Props.TEMP_UNIT, TemperatureUnits.C.value)
    device.set_property(Props.TEMP_SET, 21)
    device.set_property(Props.TEMP_BIT, 0)
    device._dirty.clear()

    # Only the half-degree bit changes; SetTem's own value is unchanged.
    device.set_property(Props.TEMP_BIT, 1)
    assert device._dirty == [Props.TEMP_BIT.value]

    commands = device._build_command_sequence()

    assert len(commands) == 1
    sent = dict(zip(commands[0]["opt"], commands[0]["p"]))
    assert sent.get(Props.TEMP_SET.value) == 21
    assert sent.get(Props.TEMP_BIT.value) == 1


def test_build_command_sequence_bundles_when_settem_changes():
    """The existing case (SetTem itself dirty) must keep working."""
    device = make_cloud_device()
    device.set_property(Props.TEMP_UNIT, TemperatureUnits.C.value)
    device.set_property(Props.TEMP_SET, 21)
    device.set_property(Props.TEMP_BIT, 0)
    device._dirty.clear()

    device.set_property(Props.TEMP_SET, 22)

    commands = device._build_command_sequence()

    assert len(commands) == 1
    sent = dict(zip(commands[0]["opt"], commands[0]["p"]))
    assert sent.get(Props.TEMP_SET.value) == 22
    assert sent.get(Props.TEMP_BIT.value) == 0


def test_build_command_sequence_orders_mode_and_power():
    device = make_cloud_device()
    device.set_property(Props.MODE, 1)
    device.set_property(Props.TEMP_SET, 21)
    device.set_property(Props.POWER, 1)
    device.set_property(Props.LIGHT, 1)

    commands = device._build_command_sequence()

    assert commands[0]["opt"] == [Props.MODE.value]
    assert commands[-1]["opt"] == [Props.POWER.value]
