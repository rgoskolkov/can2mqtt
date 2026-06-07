import logging
import json
import typing
from functools import cached_property
from abc import abstractmethod

if typing.TYPE_CHECKING:
    from .device import Device
    from .can_manager import CanManager

logger = logging.getLogger(__name__)

def bool2onoff_bytes(value: bool) -> bytes:
    """Converts a boolean to bytes 'ON' or 'OFF'."""
    return b"ON" if value else b"OFF"

def onoff_bytes2bool(value: bytes) -> bool:
    """Converts bytes 'ON' or 'OFF' to a boolean."""
    return value == b"ON"

class StateMixin:
    """A mixin for entities that report state to Home Assistant."""

    def states(self) -> typing.Generator[typing.Tuple[str, typing.Callable, typing.Any], None, None]:
        """
        Defines the state properties of this entity for MQTT discovery.
        Yields tuples of (config_key, value_formatter, type_hint).
        Example: yield "state_topic", bool2onoff_bytes, bool
        """
        yield "state_topic", str, str # Default implementation

    @cached_property
    def _states_list(self):
        return list(self.states())

    def get_mqtt_state_topic(self, state_index: int = 0) -> str:
        """Generates the unique MQTT topic for a given state."""
        return f"{self.mqtt_topic_prefix}/entity/{self.unique_id}/state/{state_index}"

    def get_mqtt_config(self) -> typing.Dict[str, typing.Any]:
        """Adds the state topics to the Home Assistant discovery payload."""
        config = super().get_mqtt_config()
        for i, (topic_key, _, _) in enumerate(self._states_list):
            config[topic_key] = self.get_mqtt_state_topic(i)
        return config

    async def publish_state(self, value: typing.Any, state_index: int = 0):
        """Publishes a state update to the correct MQTT topic."""
        topic = self.get_mqtt_state_topic(state_index)
        _, formatter, _ = self._states_list[state_index]
        payload = formatter(value)
        await self.device._mqtt_manager.publish(topic, payload, retain=True)

class CommandMixin:
    """A mixin for entities that accept commands from Home Assistant."""

    def commands(self) -> typing.Generator[typing.Tuple[str, typing.Callable, typing.Any], None, None]:
        """
        Defines the command properties of this entity for MQTT discovery.
        Yields tuples of (config_key, value_parser, type_hint).
        Example: yield "command_topic", onoff_bytes2bool, bool
        """
        yield "command_topic", str, str

    @cached_property
    def _commands_list(self):
        return list(self.commands())

    def get_mqtt_command_topic(self, command_index: int = 0) -> str:
        """Generates the unique MQTT topic for a given command."""
        return f"{self.mqtt_topic_prefix}/entity/{self.unique_id}/command/{command_index}"

    def get_mqtt_config(self) -> typing.Dict[str, typing.Any]:
        """Adds the command topics to the Home Assistant discovery payload."""
        config = super().get_mqtt_config()
        for i, (topic_key, _, _) in enumerate(self._commands_list):
            config[topic_key] = self.get_mqtt_command_topic(i)
        return config

    @abstractmethod
    async def on_mqtt_command(self, command_index: int, payload: bytes):
        """Abstract method to handle a command from MQTT."""
        raise NotImplementedError

class Entity:
    """Base class for all Home Assistant entities."""
    _entities: typing.Dict[str, 'Entity'] = {}
    TYPE_NAME: str = "base"
    PROPS: typing.Dict[str, typing.Any] = {}

    def __init__(self, device: 'Device', entity_index: int, mqtt_topic_prefix: str, name: str):
        self.device = device
        self.entity_index = entity_index
        self.mqtt_topic_prefix = mqtt_topic_prefix
        self.name = name

        self.unique_id = f"can_{self.device.node_id:03x}_{self.TYPE_NAME}_{self.entity_index:02x}"
        self._entities[self.unique_id] = self

        self.device_class: str | None = None

        # Register command topics if this is a commandable entity
        if isinstance(self, CommandMixin):
            for i in range(len(self._commands_list)):
                topic = self.get_mqtt_command_topic(i)
                self.device._mqtt_manager.register_command_entity(topic, self, i)

    @classmethod
    def get_entity_by_unique_id(cls, unique_id: str) -> typing.Optional['Entity']:
        return cls._entities.get(unique_id)

    async def publish_config(self):
        """Publishes the Home Assistant MQTT discovery configuration."""
        config_topic = self.get_mqtt_config_topic()
        config_payload = self.get_mqtt_config()
        logger.debug("MQTT config_topic: %r, payload: %r", config_topic, config_payload)
        await self.device._mqtt_manager.publish(
            config_topic, payload=json.dumps(config_payload), retain=False
        )

    async def remove_config(self):
        """Removes the Home Assistant MQTT discovery configuration."""
        config_topic = self.get_mqtt_config_topic()
        await self.device._mqtt_manager.publish(config_topic, payload=b"", retain=False)

    def get_mqtt_config_topic(self) -> str:
        """Generates the MQTT discovery topic string for this entity."""
        return f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/config"

    def get_mqtt_config(self) -> typing.Dict[str, typing.Any]:
        """Assembles the full discovery payload dictionary."""
        cfg = {
            "unique_id": self.unique_id,
            "name": self.name,
            "availability": [
                {"topic": self.device.availability_topic},
                {"topic": f"{self.mqtt_topic_prefix}/canopen2HAmqtt/status"},
            ],
            "availability_mode": "all",
            "device": {
                "identifiers": [f"canopen_node_{self.device.node_id}"],
                "name": self.device.device_name,
                "model": self.device.model_name,
            },
        }

        if self.device_class:
            cfg["device_class"] = self.device_class

        cfg.update(self.PROPS)
        return cfg

    async def mqtt_initial_publish(self):
        """Hook for entities to publish their initial state."""
        pass

    def __str__(self):
        return f"{self.__class__.__name__}(node_id={self.device.node_id}, entity_index={self.entity_index}, name='{self.name}')"

class SimpleLight(StateMixin, CommandMixin, Entity):
    """Represents a simple On/Off light."""
    TYPE_NAME = "light"

    def __init__(self, device: 'Device', entity_index: int, mqtt_topic_prefix: str, name: str, rpdo_cob_id: int):
        self._state: typing.Optional[bool] = None
        self.rpdo_cob_id = rpdo_cob_id
        super().__init__(device, entity_index, mqtt_topic_prefix, name)

    def states(self):
        yield "state_topic", bool2onoff_bytes, bool

    def commands(self):
        yield "command_topic", onoff_bytes2bool, bool

    async def set_state(self, new_state: bool):
        """Receives state from the device class and publishes to MQTT."""
        if self._state != new_state:
            self._state = new_state
            await self.publish_state(self._state)
            logger.debug("Light %s state updated to %s", self.name, "ON" if new_state else "OFF")

    async def on_mqtt_command(self, command_index: int, payload: bytes):
        """Handles commands received from MQTT for this light."""
        if command_index == 0:
            _, parser, _ = self._commands_list[0]
            new_state = parser(payload)
            logger.debug("Light %s received MQTT command: %s", self.name, "ON" if new_state else "OFF")

            # Entity sends its own CAN command
            can_payload = bytes([self.entity_index, int(new_state)])
            if self.device.can_manager:
                await self.device.can_manager.send_can_message(self.rpdo_cob_id, can_payload)
            else:
                logger.error("CAN manager not available for entity %s, cannot send command.", self.name)

    async def mqtt_initial_publish(self):
        """Publishes the current state of the light on MQTT discovery."""
        if self._state is not None:
            await self.publish_state(self._state)

class UnsupportedDeviceEntity(Entity):
    """
    A special entity for a CAN device that was detected but is not in the device registry.
    """
    TYPE_NAME = "sensor"

    def __init__(self, device: 'Device', vendor_id: int, product_code: int):
        super().__init__(device, 0, device.mqtt_topic_prefix, f"Unsupported Device (Node {device.node_id})")
        self.unique_id = f"can_unsupported_{self.device.node_id:03x}"
        self.vendor_id = vendor_id
        self.product_code = product_code
        self.state_topic: str = f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/state"
        self.attributes_topic: str = f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/attributes"

    def get_mqtt_config(self) -> typing.Dict[str, typing.Any]:
        cfg = super().get_mqtt_config()
        cfg.update({
            "icon": "mdi:help-rhombus-outline",
            "state_topic": self.state_topic,
            "json_attributes_topic": self.attributes_topic,
        })
        return cfg

    async def mqtt_initial_publish(self):
        """Publishes the device's detected IDs to MQTT."""
        state_payload = f"VID: {hex(self.vendor_id)}, PID: {hex(self.product_code)}"
        await self.device._mqtt_manager.publish(self.state_topic, state_payload, retain=True)

        attributes = {
            "node_id": self.device.node_id,
            "vendor_id": hex(self.vendor_id),
            "product_code": hex(self.product_code),
            "comment": "To support this device, add its vendor and product ID to the device registry."
        }
        await self.device._mqtt_manager.publish(self.attributes_topic, json.dumps(attributes), retain=True)


class UnconfiguredDeviceEntity(CommandMixin, Entity):
    """
    A special service entity that appears in Home Assistant for a supported device
    that has not yet been configured (e.g., has an empty device name).
    It provides a text box in the Home Assistant UI to send a configuration string
    back to the device.
    """
    TYPE_NAME = "text"
    PROPS = {
        "name": "Unconfigured Device",
        "icon": "mdi:new-box",
        "placeholder": "Enter config string (e.g., 'My New Device')",
    }

    def __init__(self, device: 'Device', entity_index: int, mqtt_topic_prefix: str, command_callback: typing.Callable[[str], typing.Awaitable[None]] = None):
        self._command_callback = command_callback
        super().__init__(device, entity_index, mqtt_topic_prefix, f"Unconfigured Device (Node {device.node_id})")

    def commands(self):
        yield "command_topic", lambda x: x.decode('utf-8').strip(), str

    async def on_mqtt_command(self, command_index: int, payload: bytes):
        """Handles commands received from MQTT for this unconfigured device."""
        if command_index == 0:
            _, parser, _ = self._commands_list[0]
            config_string = parser(payload)
            logger.debug("Unconfigured device %s received MQTT command: %s", self.name, config_string)
            if self._command_callback:
                await self._command_callback(config_string)
            else:
                logger.warning("UnconfiguredDeviceEntity received command but no callback is set.")