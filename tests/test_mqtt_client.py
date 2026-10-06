"""Tests for MQTT client module"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from greeclimate_davo22.mqtt_client import GreeMqttClient, MqttDeviceMessage


class TestMqttClient:
    """Test GreeMqttClient class"""

    @pytest.fixture
    def mqtt_client(self):
        """Create MQTT client instance"""
        return GreeMqttClient(
            user_id=12345,
            token='test_token',
            server='mqtt.gree.com',
            port=8883
        )

    def test_initialization(self, mqtt_client):
        """Test MQTT client initialization"""
        assert mqtt_client.user_id == 12345
        assert mqtt_client.token == 'test_token'
        assert mqtt_client.server == 'mqtt.gree.com'
        assert mqtt_client.port == 8883
        assert mqtt_client.is_connected is False

    def test_generate_client_id(self, mqtt_client):
        """Test client ID generation"""
        client_id = mqtt_client.get_client_id()
        assert client_id.startswith('app_')
        assert len(client_id) > 10

    def test_detect_parent_mac(self, mqtt_client):
        """Test parent MAC detection"""
        # Regular MAC (not parent/child)
        assert mqtt_client._detect_parent_mac('aabbccddeeff') == 'aabbccddeeff'

        # Child MAC (ends with 00, longer than 12 chars)
        assert mqtt_client._detect_parent_mac('aabbccddeeff00') == 'aabbccddeeff'

        # Short MAC with 00 (not parent/child)
        assert mqtt_client._detect_parent_mac('aabbccdd00') == 'aabbccdd00'

    def test_mqtt_device_message(self):
        """Test MqttDeviceMessage creation"""
        msg = MqttDeviceMessage(
            cid='12345',
            i=0,
            pack='encrypted_data',
            t='pack',
            tcid='aabbccddeeff',
            uid=12345,
            tag='tag123'
        )
        assert msg.cid == '12345'
        assert msg.i == 0
        assert msg.pack == 'encrypted_data'
        assert msg.tag == 'tag123'

    def test_add_remove_message_handler(self, mqtt_client):
        """Test adding and removing message handlers"""
        def handler(topic, message):
            pass

        mqtt_client.add_message_handler(handler)
        handler_id = id(handler)
        assert handler_id in mqtt_client._message_handlers

        mqtt_client.remove_message_handler(handler)
        assert handler_id not in mqtt_client._message_handlers


class TestMqttDeviceMessage:
    """Test MqttDeviceMessage dataclass"""

    def test_creation_minimal(self):
        """Test minimal message creation"""
        msg = MqttDeviceMessage(
            cid='123',
            i=0,
            pack='data',
            t='pack',
            tcid='mac',
            uid=123
        )
        assert msg.cid == '123'
        assert msg.tag is None
        assert msg.ts is None

    def test_creation_full(self):
        """Test full message creation"""
        msg = MqttDeviceMessage(
            cid='123',
            i=1,
            pack='data',
            t='pack',
            tcid='mac',
            uid=123,
            tag='tag123',
            ts=1234567890,
            extras={'key': 'value'}
        )
        assert msg.tag == 'tag123'
        assert msg.ts == 1234567890
        assert msg.extras == {'key': 'value'}
