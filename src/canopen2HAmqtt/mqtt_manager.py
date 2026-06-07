import logging
import typing
import aiomqtt

from typing import Dict, Tuple

from .utils import parse_mqtt_server_url, WatchdogTimer
from .config import AppConfig
import canopen2HAmqtt.entities as entities

if typing.TYPE_CHECKING:
    from .app import CanOpen2HAmqtt
    from canopen2HAmqtt.entities import Entity

logger = logging.getLogger(__name__)

class MqttManager:
    app: 'CanOpen2HAmqtt'
    
    def __init__(self, app: 'CanOpen2HAmqtt', config: AppConfig, watchdog: WatchdogTimer):
        self.app = app
        self.config = config
        self.watchdog = watchdog
        self.client: typing.Optional[aiomqtt.Client] = None
        self._command_entities: Dict[str, Tuple['Entity', int]] = {} # Maps command_topic to (entity, command_index)

    async def start(self):
        mqtt_host, auth = parse_mqtt_server_url(self.config.mqtt_server)
        will = aiomqtt.Will(f"{self.config.mqtt_topic_prefix}/canopen2HAmqtt/status", b"offline", 1, retain=True)

        logger.info("Connecting to MQTT server at %s", mqtt_host)
        async with aiomqtt.Client(mqtt_host, will=will, **auth) as client:
            self.client = client
            await self.publish_addon_status("online")
            
            async with self.client.messages() as messages:
                await self.client.subscribe(f"{self.config.mqtt_topic_prefix}/#")
                async for message in messages:
                    await self.handle_message(message)

    async def publish(self, topic: str, payload: bytes | str, retain: bool = False):
        if not self.client:
            logger.warning("MQTT client not available, cannot publish message to %s.", topic)
            return
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        try:
            await self.client.publish(topic, payload, retain=retain)
        except Exception as e:
            logger.error("Error publishing to MQTT topic %s: %s", topic, e)

    def register_command_entity(self, topic: str, entity: 'Entity', command_index: int):
        """Registers an entity to receive commands on a specific MQTT topic."""
        self._command_entities[topic] = (entity, command_index)
        logger.debug("Registered entity %s for command topic %s", entity.unique_id, topic)

    async def handle_message(self, message: aiomqtt.Message):
        if self.watchdog:
            self.watchdog.reset()

        #topic = message.topic.value.decode('utf-8')
        topic = message.topic.value
        logger.debug("Received MQTT message on topic '%s'", topic)

        # Handle Home Assistant status request (e.g., after HA restart)
        if topic == f"{self.config.mqtt_topic_prefix}/status" and message.payload == b"online":
            await self.handle_status_request()
            return

        # Route commands to registered entities
        if topic in self._command_entities:
            entity, command_index = self._command_entities[topic]
            try:
                await entity.on_mqtt_command(command_index, message.payload)
            except Exception as e:
                logger.error("Error processing MQTT command for entity %s on topic %s: %s", entity.unique_id, topic, e)
            return

    async def handle_status_request(self):
        """Handles a status request from Home Assistant to republish all configs."""
        logger.info("HA requested status update. Re-publishing all configs and states.")
        for node_id, device in self.app.can_manager.devices.items():
            if device.is_supported:
                # Re-publish discovery configs for all entities on this device
                for entity in device.entities:
                    await entity.publish_config()
                    await entity.mqtt_initial_publish()
                # Ensure device availability is "online"
                await device.publish_availability("online")
            elif node_id == UNCONFIGURED_NODE_ID and device.is_initialized:
                 # Re-publish config for unconfigured device entity
                 for entity in device.entities:
                    if isinstance(entity, entities.UnconfiguredDeviceEntity):
                        await entity.publish_config()
            else:
                 # Re-publish config for unsupported device entity
                 for entity in device.entities:
                    if isinstance(entity, entities.UnsupportedDeviceEntity):
                        await entity.publish_config()
                        await entity.mqtt_initial_publish()
        
        await self.publish_addon_status("online")

    async def publish_addon_status(self, status: str):
        """Publishes the addon's own status to MQTT."""
        status_topic = f"{self.config.mqtt_topic_prefix}/canopen2HAmqtt/status"
        await self.publish(status_topic, payload=status, retain=True)
