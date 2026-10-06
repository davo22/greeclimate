#!/usr/bin/env python3
"""
Example script demonstrating cloud device control via Gree Cloud API and MQTT.

This example shows how to:
1. Authenticate with Gree Cloud
2. Discover cloud-only devices
3. Control a cloud device via MQTT
"""

import asyncio
import logging
from greeclimate_davo22.cloud_discovery import CloudDiscovery

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

_LOGGER = logging.getLogger(__name__)


async def main():
    """Main example function"""
    
    # ============================================================
    # STEP 1: Configure credentials
    # ============================================================
    
    # Replace with your Gree Cloud credentials
    USERNAME = 'your_email@example.com'
    PASSWORD = 'your_password'
    SERVER = 'Europe'  # Options: Europe, North American, East South Asia, etc.
    
    _LOGGER.info("=" * 60)
    _LOGGER.info("Gree Cloud Device Control Example")
    _LOGGER.info("=" * 60)
    
    # ============================================================
    # STEP 2: Discover cloud devices
    # ============================================================
    
    _LOGGER.info("\n[1] Discovering cloud devices...")
    
    discovery = CloudDiscovery(
        username=USERNAME,
        password=PASSWORD,
        server=SERVER
    )
    
    try:
        # This will authenticate and scan for devices
        devices = await discovery.scan()
        
        if not devices:
            _LOGGER.error("No cloud devices found!")
            return
        
        _LOGGER.info(f"Found {len(devices)} device(s):")
        for i, dev in enumerate(devices):
            _LOGGER.info(f"  [{i}] {dev.name} ({dev.mac}) - {'online' if dev.online else 'offline'}")
        
        # ============================================================
        # STEP 3: Select and create device instance
        # ============================================================
        
        _LOGGER.info("\n[2] Connecting to first device...")
        
        # Use first device (or change index to select different device)
        selected_device_info = devices[0]
        
        # Create CloudDevice instance
        device = await discovery.create_device(
            device_info=selected_device_info,
            cipher_version=1  # Use 1 for CipherV1 (ECB), 2 for CipherV2 (GCM)
        )
        
        # Bind to device
        await device.bind()
        
        _LOGGER.info(f"Connected to: {device.device_info.name}")
        
        # ============================================================
        # STEP 4: Read current state
        # ============================================================
        
        _LOGGER.info("\n[3] Reading current state...")
        
        await device.update_state()
        
        _LOGGER.info(f"  Power: {device.power}")
        _LOGGER.info(f"  Mode: {device.mode}")
        _LOGGER.info(f"  Target Temperature: {device.target_temperature}°C")
        _LOGGER.info(f"  Current Temperature: {device.current_temperature}°C")
        _LOGGER.info(f"  Fan Speed: {device.fan_speed}")
        
        # ============================================================
        # STEP 5: Control device
        # ============================================================
        
        _LOGGER.info("\n[4] Controlling device...")
        
        # Example 1: Turn on and set temperature
        _LOGGER.info("  - Turning on device and setting to 24°C...")
        device.power = True
        device.target_temperature = 24
        await device.push_state_update()
        
        await asyncio.sleep(2)  # Wait a bit
        
        # Example 2: Change fan speed
        _LOGGER.info("  - Setting fan speed to auto...")
        device.fan_speed = 0  # 0 = Auto
        await device.push_state_update()
        
        await asyncio.sleep(2)
        
        # Example 3: Turn off
        _LOGGER.info("  - Turning off device...")
        device.power = False
        await device.push_state_update()
        
        # ============================================================
        # STEP 6: Cleanup
        # ============================================================
        
        _LOGGER.info("\n[5] Cleaning up...")
        
        await device.close()
        await discovery.close()
        
        _LOGGER.info("Done!")
    
    except Exception as e:
        _LOGGER.exception(f"Error: {e}")
    
    finally:
        # Make sure to cleanup
        try:
            await discovery.close()
        except:
            pass


if __name__ == '__main__':
    # Run the example
    asyncio.run(main())
