from canopen.objectdictionary import datatypes
from collections import defaultdict
from functools import cached_property
import logging
import json
from canopen.objectdictionary import ODVariable, ODRecord
from canopen.node import RemoteNode

from .mqtt_manager import MqttManager


class OctetString(ODVariable):
    def __init__(self, name, index, subindex):
        super().__init__(name, index, subindex)
        self.data_type = datatypes.OCTET_STRING


logger = logging.getLogger(__name__)


def bool2onoff(value):
    return b"ON" if value else b"OFF"


def onoff2bool(value):
    return value == b"ON"


class StateMixin:
    """
    A mixin for entities that report state from the CANopen device to Home Assistant.
    It manages the mapping between CANopen object dictionary variables and MQTT state topics.
    """
    _node_state_key_2_entity = defaultdict(dict)

    def states(self):
        """
        Defines the state properties of this entity.
        Each yielded tuple defines:
        1. The key in the MQTT discovery config (e.g., "state_topic").
        2. A function to format the value for MQTT (e.g., bool2onoff).
        3. The CANopen data type (e.g., datatypes.UNSIGNED8).
        """
        yield "state_topic", str, datatypes.UNSIGNED8

    @cached_property
    def STATES(self):
        return list(self.states())

    state_map = None

    @classmethod
    def get_entity_by_node_state_key(cls, node_id, state_key):
        """
        Finds which entity is responsible for a given state object on a node.
        Called by the CanManager's TPDO callback.
        - `state_key`: A unique integer representing a CANopen object (e.g., 0x21010100 for index 2101, subindex 01).
        """
        return cls._node_state_key_2_entity[node_id].get(state_key)

    def setup_state_topics(self, state_map):
        """
        Registers this entity instance to handle updates for specific state objects from the device.
        - `state_map`: A list of integer keys for the state objects this entity listens to.
        """
        self.state_map = state_map
        logger.debug("setup state topics for %s, %s", self, state_map)
        for state_key in self.state_map:
            self._node_state_key_2_entity[self.node.id][state_key] = self

    def get_mqtt_config(self):
        """Adds the state topics to the Home Assistant discovery payload."""
        config = super(StateMixin, self).get_mqtt_config()
        assert len(self.STATES) == len(self.state_map)
        for (topic, *_), state_key in zip(self.STATES, self.state_map):
            config[topic] = self.get_mqtt_state_topic(state_key)
        return config

    def get_mqtt_state_topic(self, state_key):
        """Generates the unique MQTT topic for a given state object."""
        return f"{self.mqtt_topic_prefix}/can_state_{self.node.id:03x}_{state_key:08x}"

    def get_mqtt_state(self, state_key, value):
        """
        Formats a raw value from the CAN bus into the correct format for MQTT.
        - `state_key`: The CANopen object key that changed.
        - `value`: The raw value from the device.
        - Returns: A tuple of (topic, formatted_value).
        """
        index = self.state_map.index(state_key)
        return self.get_mqtt_state_topic(state_key), self.STATES[index][1](value)

    def setup_object_dictionary(self, node, base_index):
        """
        Dynamically adds the state objects to the node's object dictionary in the `canopen` library.
        Called by DeviceManager during entity creation.
        """
        super().setup_object_dictionary(node, base_index)
        state_map = []
        index = base_index + 1
        for sub, (_, _, _type) in enumerate(self.STATES, 1):
            v = ODVariable("state", index, sub)
            v.data_type = _type
            node.object_dictionary[index].add_member(v)
            state_map.append((index << 16) | (sub << 8))
        self.setup_state_topics(state_map)


class CommandMixin:
    """
    A mixin for entities that accept commands from Home Assistant to the CANopen device.
    It manages the mapping between MQTT command topics and CANopen SDO write requests.
    """
    _mqtt_cmd_topic2entity = dict()

    def commands(self):
        """
        Defines the command properties of this entity.
        Each yielded tuple defines:
        1. The key in the MQTT discovery config (e.g., "command_topic").
        2. A function to parse the value from MQTT (e.g., onoff2bool).
        3. The CANopen data type (e.g., datatypes.UNSIGNED8).
        """
        yield "command_topic", int, datatypes.UNSIGNED8

    @cached_property
    def COMMANDS(self):
        return list(self.commands())

    command_map = None
    _topic2cmdkey = None

    @classmethod
    def get_entity_by_cmd_topic(cls, cmd_topic):
        """
        Finds which entity should handle a command from a given MQTT topic.
        Called by the MqttManager when a message is received.
        """
        return cls._mqtt_cmd_topic2entity.get(cmd_topic)

    def setup_command_topics(self, command_map):
        """
        Registers this entity instance to handle commands from specific MQTT topics.
        - `command_map`: A list of integer keys for the command objects this entity can write to.
        """
        self.command_map = command_map
        self._topic2cmdkey = {}
        for cmd_key in self.command_map:
            topic = self.get_mqtt_command_topic(cmd_key)
            self._mqtt_cmd_topic2entity[topic] = self
            self._topic2cmdkey[topic] = cmd_key

    def get_mqtt_config(self):
        """Adds the command topics to the Home Assistant discovery payload."""
        config = super(CommandMixin, self).get_mqtt_config()
        assert len(self.COMMANDS) == len(self.command_map)
        for (topic, *_), cmd_key in zip(self.COMMANDS, self.command_map):
            config[topic] = self.get_mqtt_command_topic(cmd_key)
        return config

    def get_mqtt_command_topic(self, cmd_key):
        """Generates the unique MQTT topic for a given command object."""
        return f"{self.mqtt_topic_prefix}/can_cmd_{self.node.id:03x}_{cmd_key:08x}"

    def get_can_cmd(self, topic, value):
        """
        Parses an incoming MQTT message into a CANopen SDO write request.
        - `topic`: The MQTT topic the message was received on.
        - `value`: The raw payload from MQTT (e.g., b'ON').
        - Returns: A tuple of (canopen_object_key, formatted_value).
        """
        cmd_key = self._topic2cmdkey and self._topic2cmdkey.get(topic)
        if not cmd_key:
            raise ValueError(f"topic {topic} is not recognized")
        index = self.command_map.index(cmd_key)
        return cmd_key, self.COMMANDS[index][1](value)

    def setup_object_dictionary(self, node, base_index):
        """
        Dynamically adds the command objects to the node's object dictionary.
        Called by DeviceManager during entity creation.
        """
        super().setup_object_dictionary(node, base_index)
        cmd_map = []
        index = base_index + 2
        for sub, (_, _, _type) in enumerate(self.COMMANDS, 1):
            v = ODVariable("cmd", index, sub)
            v.data_type = _type
            node.object_dictionary[index].add_member(v)
            cmd_map.append((index << 16) | (sub << 8))

        self.setup_command_topics(cmd_map)


class Entity:
    """Base class for all Home Assistant entities bridged from CANopen."""
    _entities = {}
    NAME_PROP = 1
    TYPE_ID = None
    VERSION = 0
    PROPS = {}

    @cached_property
    def METADATA_PROPERTIES(self):
        return dict(self.canopen_metadata_properties())

    def canopen_metadata_properties(self):
        yield 1, "name"
        yield 2, "device_class"

    def __init__(self, node, entity_index, mqtt_topic_prefix, caps):
        self.node = node
        self.entity_index = entity_index
        self.mqtt_topic_prefix = mqtt_topic_prefix
        self.caps = caps

        self.unique_id = f"can_{self.node.id:03x}_{self.entity_index:02x}"
        self.props = {}
        self._entities[self.unique_id] = self

    @classmethod
    def get_entity_by_unique_id(cls, unique_id):
        return cls._entities.get(unique_id)

    @classmethod
    def entities(cls):
        return list(cls._entities.values())

    @classmethod
    def remove_entity(cls, unique_id):
        cls._entities.pop(unique_id, None)

    async def publish_config(self, mqtt_manager: MqttManager):
        """
        Publishes the Home Assistant MQTT discovery configuration for this entity.
        Called by DeviceManager after an entity is created or reconfigured.
        """
        config_topic = self.get_mqtt_config_topic()
        config_payload = self.get_mqtt_config()
        logger.debug("mqtt config_topic: %r, payload: %r", config_topic, config_payload)
        await mqtt_manager.publish(
            config_topic, payload=json.dumps(config_payload), retain=False
        )

    async def remove_config(self, mqtt_manager: MqttManager):
        """
        Removes the Home Assistant MQTT discovery configuration for this entity.
        This is done by publishing an empty payload to the config topic.
        """
        config_topic = self.get_mqtt_config_topic()
        await mqtt_manager.publish(config_topic, payload=b"", retain=False)

    def set_property(self, key, value):
        self.props[key] = value

    def get_mqtt_config_topic(self):
        """
        Generates the MQTT discovery topic string for this entity.
        Example: homeassistant/light/can_01a_01/config
        """
        return f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/config"

    def get_mqtt_config(self):
        """
        Assembles the full discovery payload dictionary for Home Assistant.
        This includes availability, device info, and entity-specific properties.
        """
        cfg = {
            "unique_id": self.unique_id,
            "availability": [
                {
                    "topic": self.node.availability_topic,
                },
                {
                    "topic": f"{self.mqtt_topic_prefix}/canopen2HAmqtt/status",
                },
            ],
            "availability_mode": "all",
            "device": {
                "identifiers": [f"canopen_node_{self.node.id}"],
                "name": self.node.device_name,
                "sw_version": self.node.sw_version or "",
                "hw_version": self.node.hw_version or "",
                "manufacturer": "mrk",
                "model": self.node.device_info.get("model_name", "CANopen Device"),
            },
        }
        cfg.update(self.PROPS)
        cfg.update(self.props)
        return cfg

    def __str__(self):
        return f"{self.__class__.__name__}(node={self.node.id}, entity_index={self.entity_index})"

    def __repr__(self):
        args = []
        args.append(f"node_id=0x{self.node.id:02x}")
        args.append(f"entity_index={self.entity_index}")
        args.append(f"props={self.props}")
        args_str = ", ".join(args)
        return f"{self.__class__.__name__}({args_str})"

    def setup_object_dictionary(self, node: RemoteNode, base_index):
        node.object_dictionary.add_object(
            ODRecord(f"node {node.id:02x} metadata", base_index)
        )
        node.object_dictionary[base_index].add_member(
            OctetString("name", base_index, 1)
        )
        node.object_dictionary[base_index].add_member(
            OctetString("device_class", base_index, 2)
        )

    def set_metadata_property(self, key, value):
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        name = self.METADATA_PROPERTIES.get(key)
        if name:
            logger.debug("	%s %s: %s", name, key, value)
            self.set_property(name, value)
        else:
            logger.warning("	unknown metadata property %s: %s", key, value)

    async def mqtt_initial_publish(self, mqtt_manager: MqttManager):
        """
        Hook for entities to publish their initial state after configuration.
        Called by MqttManager on a Home Assistant status request.
        """
        pass


class EntityRegistry:
    """A registry that maps (TYPE_ID, VERSION) tuples to entity classes."""
    _by_type = {}

    @classmethod
    def register(cls, entity_class):
        """Decorator to register a new entity class."""
        cls._by_type[(entity_class.TYPE_ID, entity_class.VERSION)] = entity_class
        return entity_class

    @classmethod
    def create(cls, type_id, node, entity_index, mqtt_topic_prefix, version_override=None):
        """
        Factory method to create an entity instance based on its type and version.
        Called by DeviceManager during entity discovery.
        """
        version = (type_id >> 8) & 0xFF if version_override is None else version_override
        caps = (type_id >> 16) & 0xFFFF
        type_id = type_id & 0xFF
        logger.info("type_id: %s, version: %s, caps: %s", type_id, version, caps)

        return cls._by_type[(type_id, version)](
            node, entity_index, mqtt_topic_prefix, caps
        )




@EntityRegistry.register
class SimpleLight(StateMixin, CommandMixin, Entity):
    """
    Represents a simple On/Off light.
    - Listens for state changes from the device (via StateMixin).
    - Sends on/off commands to the device (via CommandMixin).
    """
    TYPE_ID = 5
    VERSION = 0
    TYPE_NAME = "light"

    PROPS = {"assumed_state": False}

    def states(self):
        """Defines that this light's state is a single boolean (ON/OFF)."""
        yield "state_topic", bool2onoff, datatypes.UNSIGNED8

    def commands(self):
        """Defines that this light accepts a single boolean (ON/OFF) command."""
        yield "command_topic", onoff2bool, datatypes.UNSIGNED8


@EntityRegistry.register
class UnsupportedDeviceEntity(Entity):
    """
    A special service entity that appears in Home Assistant for a CANopen device
    that was detected on the bus but is not defined in the addon's configuration.
    It provides a sensor showing the device's Vendor ID and Product Code.
    """
    TYPE_ID = 253
    VERSION = 0
    TYPE_NAME = "sensor"

    def __init__(self, node, entity_index, mqtt_topic_prefix, caps):
        super().__init__(node, entity_index, mqtt_topic_prefix, caps)
        self.unique_id = f"can_unsupported_{self.node.id:03x}"
        self.vendor_id = 0
        self.product_code = 0

    def get_mqtt_config(self):
        cfg = super().get_mqtt_config()
        cfg.update({
            "name": f"Unsupported Device (Node {self.node.id})",
            "icon": "mdi:help-rhombus-outline",
            "state_topic": f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/state",
            "json_attributes_topic": f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/attributes",
        })
        return cfg

    async def mqtt_initial_publish(self, mqtt_manager):
        """Publishes the device's detected IDs to MQTT for display in Home Assistant."""
        state_payload = f"VID: {hex(self.vendor_id)}, PID: {hex(self.product_code)}"
        await mqtt_manager.publish(
            f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/state",
            state_payload,
            retain=True
        )

        attributes = {
            "node_id": self.node.id,
            "vendor_id": hex(self.vendor_id),
            "product_code": hex(self.product_code),
            "comment": "To support this device, add its vendor and product ID to the devices list in the addon configuration."
        }
        await mqtt_manager.publish(
            f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/attributes",
            json.dumps(attributes),
            retain=True
        )

@EntityRegistry.register
class UnconfiguredDeviceEntity(CommandMixin, Entity):
    """
    A special service entity that appears in Home Assistant for a supported device
    that has not yet been configured (e.g., has an empty device name).
    It provides a text box in the Home Assistant UI to send a configuration string
    (e.g., a name) back to the device.
    """
    TYPE_ID = 254
    TYPE_NAME = "text"
    PROPS = {
        "name": "Unconfigured Device",
        "icon": "mdi:new-box",
        "placeholder": "Enter config string (e.g., 'My New Device')",
    }

    def commands(self):
        """Defines that this entity accepts a string command from Home Assistant."""
        yield "command_topic", str, datatypes.OCTET_STRING

