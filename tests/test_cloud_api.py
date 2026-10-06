"""Tests for cloud API module"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import base64
import json

from greeclimate_davo22.cloud_api import (
    GreeCloudApi,
    CloudDeviceInfo,
    CloudHome,
    CloudCredentials,
    GREE_CLOUD_SERVERS,
)


class TestCloudApi:
    """Test GreeCloudApi class"""

    @pytest.fixture
    async def api(self):
        """Create API instance"""
        api_instance = GreeCloudApi('https://eugrih.gree.com', 'test@example.com', 'password')
        yield api_instance
        # Cleanup
        await api_instance.close()

    @pytest.mark.asyncio
    async def test_for_server(self):
        """Test server factory method"""
        async with GreeCloudApi.for_server('Europe', 'test@example.com', 'password') as api:
            assert api.base_url == 'https://eugrih.gree.com'
            assert api.username == 'test@example.com'

    def test_for_server_invalid(self):
        """Test invalid server raises error"""
        with pytest.raises(ValueError):
            GreeCloudApi.for_server('InvalidServer', 'test@example.com', 'password')

    @pytest.mark.asyncio
    async def test_md5(self):
        """Test MD5 hashing"""
        async with GreeCloudApi('https://eugrih.gree.com', 'test@example.com', 'password') as api:
            result = api._md5('test')
            assert result == '098f6bcd4621d373cade4e832627b4f6'

    @pytest.mark.asyncio
    async def test_encrypt_decrypt(self):
        """Test encryption and decryption"""
        async with GreeCloudApi('https://eugrih.gree.com', 'test@example.com', 'password') as api:
            data = 'test data'
            encrypted = api._encrypt(data)
            decrypted = api._decrypt(encrypted)
            assert decrypted == data

    def test_gree_cloud_servers(self):
        """Test that all expected servers are defined"""
        assert 'Europe' in GREE_CLOUD_SERVERS
        assert 'North American' in GREE_CLOUD_SERVERS
        assert 'China Mainland' in GREE_CLOUD_SERVERS
        assert len(GREE_CLOUD_SERVERS) >= 9

    @pytest.mark.asyncio
    async def test_login_success(self, api):
        """Test successful login"""
        mock_response = {
            'uid': 12345,
            'token': 'test_token_123'
        }

        with patch.object(api, '_send_request', new=AsyncMock()) as mock_send:
            encrypted = base64.b64encode(
                api._encrypt(json.dumps(mock_response))
            ).decode()
            mock_send.return_value = encrypted

            credentials = await api.login()

            assert credentials.user_id == 12345
            assert credentials.token == 'test_token_123'
            assert api.user_id == 12345
            assert api.token == 'test_token_123'

    @pytest.mark.asyncio
    async def test_get_homes(self, api):
        """Test getting homes list"""
        api.user_id = 12345
        api.token = 'test_token'

        mock_response = {
            'home': [
                {'id': 1, 'name': 'Home 1'},
                {'id': 2, 'name': 'Home 2'}
            ]
        }

        with patch.object(api, '_send_request', new=AsyncMock()) as mock_send:
            encrypted = base64.b64encode(
                api._encrypt(json.dumps(mock_response))
            ).decode()
            mock_send.return_value = encrypted

            homes = await api.get_homes()

            assert len(homes) == 2
            assert homes[0].id == 1
            assert homes[0].name == 'Home 1'
            assert homes[1].id == 2
            assert homes[1].name == 'Home 2'

    @pytest.mark.asyncio
    async def test_get_homes_not_logged_in(self, api):
        """Test get_homes fails when not logged in"""
        with pytest.raises(Exception, match='Not logged in'):
            await api.get_homes()

    @pytest.mark.asyncio
    async def test_get_devices(self, api):
        """Test getting devices list"""
        api.user_id = 12345
        api.token = 'test_token'

        mock_response = {
            'rooms': [
                {
                    'devs': [
                        {
                            'name': 'Living Room AC',
                            'mac': 'aabbccddeeff',
                            'key': 'testkey123456789',
                            'model': 'Model-X',
                            'ver': 'V1.0'
                        }
                    ]
                }
            ]
        }

        with patch.object(api, '_send_request', new=AsyncMock()) as mock_send:
            encrypted = base64.b64encode(
                api._encrypt(json.dumps(mock_response))
            ).decode()
            mock_send.return_value = encrypted

            devices = await api.get_devices(1)

            assert len(devices) == 1
            assert devices[0].name == 'Living Room AC'
            assert devices[0].mac == 'aabbccddeeff'
            assert devices[0].key == 'testkey123456789'

    @pytest.mark.asyncio
    async def test_get_all_devices_filters_duplicates(self, api):
        """Test that get_all_devices filters duplicate devices, keeping the one with '00' MAC suffix"""
        api.user_id = 12345
        api.token = 'test_token'

        # Mock get_homes to return one home
        with patch.object(api, 'get_homes', new=AsyncMock()) as mock_homes:
            mock_homes.return_value = [CloudHome(id=1, name='Home 1')]

            # Mock get_devices to return duplicates (same key, different MACs)
            with patch.object(api, 'get_devices', new=AsyncMock()) as mock_devices:
                mock_devices.return_value = [
                    CloudDeviceInfo(
                        name='AC Non-Responsive',
                        mac='c03937a616ab',  # MAC without '00' - doesn't respond
                        key='sharedkey123',
                        online=True
                    ),
                    CloudDeviceInfo(
                        name='AC Responsive',
                        mac='c03937a616ab00',  # MAC with '00' suffix - responds
                        key='sharedkey123',  # Same key as above
                        online=True
                    ),
                    CloudDeviceInfo(
                        name='AC Other',
                        mac='aabbccddeeff',  # Different device
                        key='differentkey456',
                        online=True
                    )
                ]

                devices = await api.get_all_devices()

                # Should filter out the device without '00' and keep the one with '00'
                assert len(devices) == 2
                assert not any(d.mac == 'c03937a616ab' for d in devices)
                assert any(d.mac == 'c03937a616ab00' for d in devices)
                assert any(d.mac == 'aabbccddeeff' for d in devices)

    @pytest.mark.asyncio
    async def test_filter_duplicate_devices_no_duplicates(self, api):
        """Test _filter_duplicate_devices with no duplicates"""
        devices = [
            CloudDeviceInfo(name='AC 1', mac='aabbccddeeff', key='key1'),
            CloudDeviceInfo(name='AC 2', mac='112233445566', key='key2'),
        ]

        filtered = api._filter_duplicate_devices(devices)
        assert len(filtered) == 2

    @pytest.mark.asyncio
    async def test_filter_duplicate_devices_with_00_suffix(self, api):
        """Test _filter_duplicate_devices keeps device with '00' suffix"""
        devices = [
            CloudDeviceInfo(name='Non-Responsive', mac='c03937a616ab', key='sharedkey'),
            CloudDeviceInfo(name='Responsive', mac='c03937a616ab00', key='sharedkey'),
        ]

        filtered = api._filter_duplicate_devices(devices)
        assert len(filtered) == 1
        assert filtered[0].mac == 'c03937a616ab00'

    @pytest.mark.asyncio
    async def test_filter_duplicate_devices_only_with_00(self, api):
        """Test _filter_duplicate_devices when only device with '00' exists"""
        devices = [
            CloudDeviceInfo(name='Device with 00', mac='c03937a616ab00', key='key1'),
        ]

        # Should keep it since there's no duplicate
        filtered = api._filter_duplicate_devices(devices)
        assert len(filtered) == 1

    @pytest.mark.asyncio
    async def test_filter_duplicate_devices_multiple_without_00(self, api):
        """Test _filter_duplicate_devices with multiple devices without '00' sharing key"""
        devices = [
            CloudDeviceInfo(name='AC 1', mac='aabbccddeeff', key='sharedkey'),
            CloudDeviceInfo(name='AC 2', mac='112233445566', key='sharedkey'),
        ]

        # Both without '00' suffix, should keep both (no device with '00' to prefer)
        filtered = api._filter_duplicate_devices(devices)
        assert len(filtered) == 2


class TestCloudDataClasses:
    """Test cloud data classes"""

    def test_cloud_device_info(self):
        """Test CloudDeviceInfo creation"""
        device = CloudDeviceInfo(
            name='Test AC',
            mac='aabbccddeeff',
            key='testkey123',
            model='Model-X',
            version='V1.0',
            online=True
        )
        assert device.name == 'Test AC'
        assert device.mac == 'aabbccddeeff'
        assert device.online is True

    def test_cloud_home(self):
        """Test CloudHome creation"""
        home = CloudHome(id=123, name='My Home')
        assert home.id == 123
        assert home.name == 'My Home'

    def test_cloud_credentials(self):
        """Test CloudCredentials creation"""
        creds = CloudCredentials(user_id=12345, token='test_token')
        assert creds.user_id == 12345
        assert creds.token == 'test_token'
