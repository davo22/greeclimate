"""Tests for cloud discovery module"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from greeclimate_davo22.cloud_discovery import CloudDiscovery
from greeclimate_davo22.cloud_api import CloudDeviceInfo, CloudHome, CloudCredentials


class TestCloudDiscovery:
    """Test CloudDiscovery class"""

    @pytest.fixture
    def discovery(self):
        """Create discovery instance"""
        return CloudDiscovery(
            username='test@example.com',
            password='password',
            server='Europe'
        )

    def test_initialization(self, discovery):
        """Test discovery initialization"""
        assert discovery.username == 'test@example.com'
        assert discovery.password == 'password'
        assert discovery.server == 'Europe'
        assert discovery._authenticated is False
        assert len(discovery.devices) == 0

    def test_list_servers(self):
        """Test listing available servers"""
        servers = CloudDiscovery.list_servers()
        assert 'Europe' in servers
        assert 'North American' in servers
        assert len(servers) >= 9

    @pytest.mark.asyncio
    async def test_authenticate(self, discovery):
        """Test authentication"""
        mock_credentials = CloudCredentials(user_id=12345, token='test_token')

        with patch('greeclimate_davo22.cloud_discovery.GreeCloudApi') as MockApi, \
             patch('greeclimate_davo22.cloud_discovery.GreeMqttClient') as MockMqtt:

            mock_api = MagicMock()
            mock_api.login = AsyncMock(return_value=mock_credentials)
            MockApi.for_server.return_value = mock_api

            mock_mqtt = MagicMock()
            mock_mqtt.connect = AsyncMock()
            MockMqtt.return_value = mock_mqtt

            await discovery.authenticate()

            assert discovery._authenticated is True
            assert discovery._api == mock_api
            assert discovery._mqtt_client == mock_mqtt
            mock_api.login.assert_called_once()
            mock_mqtt.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_scan(self, discovery):
        """Test device scanning"""
        mock_credentials = CloudCredentials(user_id=12345, token='test_token')
        mock_devices = [
            CloudDeviceInfo(
                name='AC 1',
                mac='aabbccddeeff',
                key='key123',
                online=True
            ),
            CloudDeviceInfo(
                name='AC 2',
                mac='112233445566',
                key='key456',
                online=False
            )
        ]

        with patch('greeclimate_davo22.cloud_discovery.GreeCloudApi') as MockApi, \
             patch('greeclimate_davo22.cloud_discovery.GreeMqttClient') as MockMqtt:

            mock_api = MagicMock()
            mock_api.login = AsyncMock(return_value=mock_credentials)
            mock_api.get_all_devices = AsyncMock(return_value=mock_devices)
            MockApi.for_server.return_value = mock_api

            mock_mqtt = MagicMock()
            mock_mqtt.connect = AsyncMock()
            MockMqtt.return_value = mock_mqtt

            devices = await discovery.scan()

            assert len(devices) == 2
            assert devices[0].name == 'AC 1'
            assert devices[1].name == 'AC 2'
            assert discovery.devices == devices

    @pytest.mark.asyncio
    async def test_scan_authenticates_if_needed(self, discovery):
        """Test scan authenticates automatically"""
        mock_credentials = CloudCredentials(user_id=12345, token='test_token')

        with patch('greeclimate_davo22.cloud_discovery.GreeCloudApi') as MockApi, \
             patch('greeclimate_davo22.cloud_discovery.GreeMqttClient') as MockMqtt:

            mock_api = MagicMock()
            mock_api.login = AsyncMock(return_value=mock_credentials)
            mock_api.get_all_devices = AsyncMock(return_value=[])
            MockApi.for_server.return_value = mock_api

            mock_mqtt = MagicMock()
            mock_mqtt.connect = AsyncMock()
            MockMqtt.return_value = mock_mqtt

            assert discovery._authenticated is False
            await discovery.scan()
            assert discovery._authenticated is True

    @pytest.mark.asyncio
    async def test_close(self, discovery):
        """Test cleanup"""
        mock_mqtt = MagicMock()
        mock_mqtt.disconnect = AsyncMock()
        discovery._mqtt_client = mock_mqtt
        discovery._authenticated = True

        await discovery.close()

        mock_mqtt.disconnect.assert_called_once()
        assert discovery._mqtt_client is None
        assert discovery._authenticated is False

    def test_repr(self, discovery):
        """Test string representation"""
        repr_str = repr(discovery)
        assert 'CloudDiscovery' in repr_str
        assert 'Europe' in repr_str
        assert 'devices=0' in repr_str
