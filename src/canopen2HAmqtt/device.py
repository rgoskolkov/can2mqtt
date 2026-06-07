import time
import logging
import struct
from abc import ABC, abstractmethod

from .entities import Entity, SimpleLight, UnconfiguredDeviceEntity, UnsupportedDeviceEntity
from .mqtt_manager import MqttManager

logger = logging.getLogger(__name__)

UNCONFIGURED_NODE_ID = 0

class Device(ABC):
    """
    Abstract base class for a CAN device.
    It holds all application-specific state and logic for a node.
    """
    _mqtt_manager: MqttManager = None

    @classmethod
    def set_mqtt_manager(cls, manager: MqttManager):
        cls._mqtt_manager = manager

    def __init__(self, mqtt_topic_prefix: str, node_id: int, model_name: str):
        self.mqtt_topic_prefix = mqtt_topic_prefix
        self.node_id = node_id
        self.model_name = model_name
        self.is_supported = True
        self.can_manager = None
        self.is_initialized: bool = False
        self.last_heartbeat_time: float = time.time()
        self.availability: str = "offline"
        self.prod_heartbeat_time: int | None = None
        self.device_name: str = f"CANopen Node {self.node_id}"
        self.entities: list[Entity] = []

        # --- MQTT Topics ---
        self.availability_topic: str = f"{self.mqtt_topic_prefix}/can_{self.node_id:03x}/availability"

    async def publish_availability(self, availability: str):
        """Publishes the node's availability to MQTT, if it has changed."""
        if self.availability != availability:
            logger.info("Node %02x is now %s", self.node_id, availability)
            self.availability = availability
            
            if not self._mqtt_manager:
                logger.error("MqttManager not set for Device class. Cannot publish availability.")
                return

            await self._mqtt_manager.publish(self.availability_topic, payload=availability, retain=True)

    @abstractmethod
    async def initialize(self, can_manager):
        """
        Device-specific initialization.
        This method should create entities and register CAN message handlers.
        """
        self.can_manager = can_manager
        raise NotImplementedError

    @abstractmethod
    def handle_pdo(self, can_id: int, data: bytes):
        """
        Device-specific handler for incoming PDO messages.
        """
        raise NotImplementedError

    def __str__(self):
        return f"Device(id={self.node_id}, name='{self.device_name}')"

    def __repr__(self):
        return str(self)


class BluePillRelayDevice(Device):
    """
    Specific implementation for the 8-channel BluePill relay board.
    """
    MODEL_NAME = "BluePill 8-channel Relay"
    TPDO1_COB_ID_BASE = 0x180
    RPDO1_COB_ID_BASE = 0x200

    def __init__(self, mqtt_topic_prefix: str, node_id: int):
        super().__init__(mqtt_topic_prefix, node_id, self.MODEL_NAME)
        self.prod_heartbeat_time = 10000
        self.tpdo1_cob_id = self.TPDO1_COB_ID_BASE + self.node_id
        self.rpdo1_cob_id = self.RPDO1_COB_ID_BASE + self.node_id

    async def initialize(self, can_manager):
        self.can_manager = can_manager
        if self.node_id == UNCONFIGURED_NODE_ID:
            logger.info("Device is unconfigured (Node ID 0). Creating configuration entity.")
            config_entity = UnconfiguredDeviceEntity(self, 0, self.mqtt_topic_prefix, command_callback=self._set_node_id)
            self.entities.append(config_entity)
            await config_entity.publish_config()
        else:
            self.can_manager.register_pdo_handler(self.tpdo1_cob_id, self)
            logger.info("Registered TPDO handler for 0x%03x", self.tpdo1_cob_id)

            for i in range(8):
                entity = SimpleLight(
                    device=self,
                    entity_index=i,
                    mqtt_topic_prefix=self.mqtt_topic_prefix,
                    name=f"Relay {i + 1}",
                    rpdo_cob_id=self.rpdo1_cob_id
                )
                self.entities.append(entity)
            
            for entity in self.entities:
                await entity.publish_config()

            logger.info("Initialized %d entities for %s", len(self.entities), self.device_name)

    async def _set_node_id(self, new_node_id_str: str):
        """Callback to set a new node ID via SDO."""
        try:
            new_node_id = int(new_node_id_str)
            if not (1 <= new_node_id <= 127):
                raise ValueError("Node ID must be between 1 and 127.")
            
            logger.info("Attempting to assign new node ID %d to unconfigured device.", new_node_id)
            success = await self.can_manager._write_node_id(UNCONFIGURED_NODE_ID, struct.pack('<B', new_node_id))

            if success:
                logger.info("Successfully sent command to assign new node ID. The device will reset and reappear as node %d.", new_node_id)
            else:
                logger.error("Failed to assign new node ID %d.", new_node_id)

        except ValueError as e:
            logger.error("Invalid Node ID provided for unconfigured device: %s", e)

    async def handle_pdo(self, can_id: int, data: bytes):
        """
        Handle an incoming TPDO message from the relay board.
        The first byte of the TPDO is assumed to be the bitmask of relay states.
        """
        if can_id != self.tpdo1_cob_id:
            return

        if not data:
            logger.warning("Received empty TPDO for %s", self.device_name)
            return

        new_states = data[0]
        
        for i, entity in enumerate(self.entities):
            if isinstance(entity, SimpleLight):
                new_state = (new_states >> i) & 1
                await entity.set_state(bool(new_state))

class UnsupportedDevice(Device):
    """A device that is not in the registry. It creates a single entity to display its info."""
    
    def __init__(self, mqtt_topic_prefix: str, node_id: int, product_code: int):
        super().__init__(mqtt_topic_prefix, node_id, "Unsupported Device")
        self.product_code = product_code
        self.is_supported = False
        self.prod_heartbeat_time = 1000 # Use a default for availability checking

    async def initialize(self, can_manager):
        self.can_manager = can_manager
        logger.debug("UnsupportedDevice initialized for node %d.", self.node_id)
        
        # Create and publish the special entity for unsupported devices
        unsupported_entity = UnsupportedDeviceEntity(self, 0, self.product_code)
        self.entities.append(unsupported_entity)
        
        await unsupported_entity.publish_config()
        await unsupported_entity.mqtt_initial_publish()

    def handle_pdo(self, can_id: int, data: bytes):
        # Unsupported device does not handle any PDOs
        pass
