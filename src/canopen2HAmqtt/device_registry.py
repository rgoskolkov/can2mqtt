from typing import Dict, Type

from .device import Device, BluePillRelayDevice

# Found in the firmware source code of blue-pill-relay-can
PRODUCT_CODE_BLUEPILL = 0x0000F103

DEVICE_REGISTRY: Dict[int, Type[Device]] = {
    PRODUCT_CODE_BLUEPILL: BluePillRelayDevice,
    # Add other known devices here if needed, keyed by their Product Code
}
