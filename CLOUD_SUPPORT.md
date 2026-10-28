# Gree Cloud Support

This document describes the cloud-only device support added to greeclimate.

## Overview

Gree manufactures devices that only work through their cloud service and do not respond to local UDP discovery. This implementation adds support for controlling these cloud-only devices via the Gree Cloud API and MQTT broker.

## Features

- **Cloud Authentication**: Login to Gree Cloud with email/password
- **Device Discovery**: Retrieve list of cloud devices with encryption keys
- **MQTT Communication**: Control devices via Gree Cloud MQTT broker
- **Multiple Regions**: Support for all Gree Cloud regions (Europe, US, Asia, etc.)
- **Parent/Child Devices**: Support for commercial units with parent/child hierarchy
- **Both Cipher Versions**: Support for CipherV1 (ECB) and CipherV2 (GCM)

## Architecture

The cloud implementation follows clean architecture principles:

```
greeclimate_davo22/
├── cloud_api.py          # Gree Cloud REST API client
├── mqtt_client.py        # MQTT broker communication (modules with protocol >= 3.2)
├── cloud_gateway.py      # Legacy TLS gateway (modules with protocol < 3.2)
├── cloud_device.py       # Cloud device implementation
└── cloud_discovery.py    # Cloud device discovery
```

### Two transports

The Gree+ app reaches a module in one of two ways, depending on the protocol
version in its cloud record (`ver`, e.g. `V3.5.0`):

- **`ver >= 3.2` → MQTT.** Requests go to `request/<mac>`, replies arrive on
  `response/<mac>/#` and `status/<mac>/#`.
- **`ver < 3.2` → legacy gateway.** These modules never connect to the MQTT
  broker. `/App/QueryOnline` returns the gateway each device is attached to
  (`svr`, `isSSL`); the client opens a TLS connection to it, sends one
  newline-terminated envelope with the inner pack AES-128-ECB-encrypted under
  the device key, reads one line back and closes. See `cloud_gateway.py`.

`CloudDevice` picks the transport automatically from `device_info.version`
when it is given a shared `GreeCloudGateway`; without one it always uses MQTT
(the behaviour of releases before 2.4.0). Pass `transport="mqtt"` or
`transport="gateway"` to force one.

### Key Differences from Local Devices

| Feature | Local Device | Cloud Device |
|---------|--------------|--------------|
| Protocol | UDP | MQTT over TLS, or TLS gateway for pre-3.2 modules |
| Discovery | Broadcast | Cloud API |
| Temperature | Offset +40 | Raw values |
| Commands | Batch | Sequential |
| Authentication | Bind key | Cloud token |

## Usage

### Basic Example

```python
import asyncio
from greeclimate_davo22.cloud_discovery import CloudDiscovery

async def main():
    # Create discovery instance
    discovery = CloudDiscovery(
        username='your_email@example.com',
        password='your_password',
        server='Europe'
    )
    
    # Discover devices
    devices = await discovery.scan()
    
    # Create device instance
    device = await discovery.create_device(devices[0])
    await device.bind()
    
    # Control device
    await device.update_state()
    device.power = True
    device.target_temperature = 24
    await device.push_state_update()
    
    # Cleanup
    await device.close()
    await discovery.close()

asyncio.run(main())
```

### Available Cloud Servers

- `Australia` - https://augrih.gree.com
- `China Mainland` - https://grih.gree.com
- `East South Asia` - https://hkgrih.gree.com
- `Europe` - https://eugrih.gree.com
- `India` - https://ingrih.gree.com
- `Latin American` - https://lagrih.gree.com
- `Middle East` - https://megrih.gree.com
- `North American` - https://nagrih.gree.com
- `Russia` - https://rugrih.gree.com
- `South American` - https://sagrih.gree.com

### Advanced Example with Manual Setup

```python
from greeclimate_davo22.cloud_api import GreeCloudApi
from greeclimate_davo22.mqtt_client import GreeMqttClient
from greeclimate_davo22.cloud_device import CloudDevice
from greeclimate_davo22.deviceinfo import DeviceInfo

# Step 1: Authenticate with Cloud API
api = GreeCloudApi.for_server('Europe', 'email@example.com', 'password')
credentials = await api.login()

# Step 2: Get devices
devices = await api.get_all_devices()
device_info = devices[0]

# Step 3: Create MQTT client
mqtt = GreeMqttClient(credentials.user_id, credentials.token)
await mqtt.connect()

# Step 4: Create device
dev_info = DeviceInfo(
    ip='mqtt.gree.com',
    port=8883,
    mac=device_info.mac,
    name=device_info.name
)

device = CloudDevice(
    mqtt_client=mqtt,
    device_info=dev_info,
    device_key=device_info.key,
    cipher_version=1  # 1 = CipherV1 (ECB), 2 = CipherV2 (GCM)
)

await device.bind()
await device.update_state()

# Control device
device.power = True
await device.push_state_update()
```

## API Reference

### CloudDiscovery

Main class for cloud device discovery and management.

```python
discovery = CloudDiscovery(username, password, server='Europe')
await discovery.scan()  # Returns list of CloudDeviceInfo
device = await discovery.create_device(device_info)
await discovery.close()
```

### GreeCloudApi

Low-level Cloud API client.

```python
api = GreeCloudApi.for_server('Europe', username, password)
credentials = await api.login()
homes = await api.get_homes()
devices = await api.get_devices(home_id)
all_devices = await api.get_all_devices()
```

### GreeMqttClient

MQTT broker client for device communication.

```python
mqtt = GreeMqttClient(user_id, token)
await mqtt.connect()
await mqtt.subscribe_to_device(device_mac)
await mqtt.publish_command(device_mac, command, cipher)
await mqtt.disconnect()
```

### CloudDevice

Cloud-based device controller (extends Device).

```python
gateway = GreeCloudGateway(api)  # shared by all devices of the account
device = CloudDevice(mqtt_client, device_info, device_key, gateway=gateway)
await device.bind()
await device.update_state()      # raises asyncio.TimeoutError if the device stays silent
device.power = True
device.target_temperature = 24
await device.push_state_update()
await device.close()
```

`device.transport` tells you which path was chosen (`"mqtt"` or `"gateway"`).
Since 2.4.0 `update_state()` raises `asyncio.TimeoutError` (or its subclass
`DeviceOfflineError`) instead of logging and returning, so callers can count
failures and mark the device unavailable.

## Important Notes

### Temperature Handling

Cloud devices do **NOT** use the +40 temperature offset that local devices use:

- **Local Device**: 24°C → send as -16 (24 - 40)
- **Cloud Device**: 24°C → send as 24

### Command Sequencing

Cloud devices require sequential command execution:

```python
# ✅ Correct - wait between commands
device.mode = 1
await device.push_state_update()

device.target_temperature = 24
await device.push_state_update()

# ❌ Wrong - batch update may fail
device.mode = 1
device.target_temperature = 24
await device.push_state_update()
```

### Parent/Child Devices and Duplicate Filtering

Commercial units may have parent/child hierarchy:

- **Parent MAC**: Base address (e.g., `c03937a616ab`)
- **Child MAC**: Parent + suffix (e.g., `c03937a616ab00`)
- **Topics**: Use parent MAC
- **Target (tcid)**: Use child MAC

This is handled automatically by `CloudDevice`.

**Important**: Cloud API sometimes returns duplicate devices with the same encryption key but different MAC addresses (one normal, one with '00' appended). The device **without** '00' suffix doesn't respond to commands. The `CloudDiscovery.scan()` method automatically filters out these non-responsive duplicates, keeping only the device with '00' suffix that actually responds to commands.

### Cipher Versions

- **CipherV1** (default): AES-128-ECB - used by most devices
- **CipherV2**: AES-128-GCM - used by some newer devices

Try CipherV1 first, if that doesn't work try CipherV2.

## Troubleshooting

### Connection Issues

1. Verify credentials are correct
2. Check selected server region
3. Ensure firewall allows MQTT port 8883
4. Enable debug logging: `logging.basicConfig(level=logging.DEBUG)`

### Commands Not Working

1. Wait for response between commands
2. Ensure device is online (check `device.online`)
3. Try opposite cipher version
4. Check MQTT logs for errors

### Decryption Failures

Some response messages may fail to decrypt - this is normal for certain device types. As long as status updates work, the device is functioning correctly.

## Dependencies

New dependencies required for cloud support:

```
aiohttp>=3.8.0    # For Cloud API HTTP requests
aiomqtt>=1.0.0    # For MQTT communication
```

Install with:

```bash
pip install -r requirements.txt
```

## Testing

Run the example script:

```bash
python example_cloud.py
```

## Credits

Cloud protocol reverse engineering based on:
- [luc10/gree-api-client](https://github.com/luc10/gree-api-client)
- PCAP analysis of Gree+ mobile app
- Community contributions

## License

Same as greeclimate - GPL-3.0
