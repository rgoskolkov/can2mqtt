import logging
import aiomqtt
from .app import CanOpen2HAmqtt

from .entities import CommandMixin, Entity, UnconfiguredDeviceEntity
from .utils import parse_mqtt_server_url

logger = logging.getLogger(__name__)

class MqttManager:
    app: CanOpen2HAmqtt
    def __init__(self, app):
        self.app = app
        self.client = None

    async def start(self):
        """
        Connects to the MQTT broker and starts the listener.
        This is the single entry point for all MQTT-related operations.
        """
        mqtt_host, auth = parse_mqtt_server_url(self.app.mqtt_server)
        will = aiomqtt.Will(f"{self.app.mqtt_topic_prefix}/canopen2HAmqtt/status", b"offline", 1, retain=True)

        logger.info("Connecting to MQTT server at %s", mqtt_host)
        async with aiomqtt.Client(mqtt_host, will=will, **auth) as client:
            self.client = client

            await self.publish_addon_status("online")
            
            async with self.client.messages() as messages:
                await self.client.subscribe(f"{self.app.mqtt_topic_prefix}/#")
                async for message in messages:
                    await self.handle_message(message)

    async def publish(self, topic, payload, retain=False):
        if not self.client:
            logger.warning("MQTT client not available, cannot publish message.")
            return
        await self.client.publish(topic, payload, retain=retain)

    async def handle_message(self, message):
        if self.app.main_watchdog:
            self.app.main_watchdog.reset()

        topic = message.topic.value
        logger.debug("Received MQTT message on topic '%s'", topic)

        if topic == f"{self.app.mqtt_topic_prefix}/status" and message.payload == b"online":
            await self.handle_status_request()
            return

        entity = CommandMixin.get_entity_by_cmd_topic(topic)
        if not entity:
            return

        if isinstance(entity, UnconfiguredDeviceEntity):
            await self.handle_unconfigured_device_command(entity, message)
        else:
            await self.handle_entity_command(entity, topic, message)

    async def handle_status_request(self):
        """Handles a status request from Home Assistant to republish all configs."""
        logger.info("HA requested status update. Re-publishing all configs and states.")
        for entity in Entity.entities():
            await entity.publish_config(self)
            await entity.mqtt_initial_publish(self)
        for node in self.app.can_manager.get_nodes():
            if getattr(node, 'is_supported', False):
                await self.publish(node.availability_topic, payload="online", retain=True)
        await self.publish_addon_status("online")

    async def handle_unconfigured_device_command(self, entity, message):
        """Handles the command for an unconfigured device to set its name."""
        try:
            device_name = message.payload.decode('utf-8').strip()
            if not device_name: return
            logger.info("Applying configuration to node %02x: set name to '%s'", entity.node.id, device_name)
            await entity.node.sdo[0x1008].aset_raw(device_name.encode('utf-8'))
            await self.app.device_manager.process_node_entities(entity.node)
            await entity.remove_config(self)
            Entity.remove_entity(entity.unique_id)
        except Exception as e:
            logger.error("Failed to apply configuration for node %02x: %s", entity.node.id, e)

    async def handle_entity_command(self, entity, topic, message):
        """Handles a standard command for a configured entity."""
        try:
            cmd_key, value = entity.get_can_cmd(topic, message.payload)
            var = entity.node.sdo[cmd_key >> 16][(cmd_key >> 8) & 0xFF]
            await var.aset_raw(value)
            logger.debug("Sent command to %r: key=%08x, value=%s", entity, cmd_key, value)
        except Exception as e:
            logger.error("Error processing command for %r: %s", entity, e)

    async def publish_addon_status(self, status):
        """Publishes the addon's own status to MQTT."""
        status_topic = f"{self.app.mqtt_topic_prefix}/canopen2HAmqtt/status"
        await self.publish(status_topic, payload=status, retain=True)
