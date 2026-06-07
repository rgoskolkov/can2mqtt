import logging

from canopen.sdo.exceptions import SdoAbortedError, SdoCommunicationError
from .app import CanOpen2HAmqtt

from .entities import Entity, EntityRegistry, UnconfiguredDeviceEntity, UnsupportedDeviceEntity
from .utils import async_try_iter_items

logger = logging.getLogger(__name__)

class DeviceManager:
    app: CanOpen2HAmqtt
    def __init__(self, app):
        self.app = app

    async def process_node_entities(self, node):
        """Reads entity definitions from a device and creates/updates them in Home Assistant."""
        logger.info("Processing entities for node %02x...", node.id)
        try:
            # Read basic info
            node.sw_version = await node.sdo["SoftwareVersion"].aget_raw()
            node.device_name = await node.sdo[0x1008].aget_raw()
            node.hw_version = await node.sdo[0x1009].aget_raw()
        except (SdoAbortedError, SdoCommunicationError) as e:
            logger.warning("Node %02x: Could not read one or more basic info properties. Using defaults. Error: %s", node.id, e)

        # Remove existing entities before re-discovery
        for entity in list(Entity.entities()):
            if entity.node.id == node.id and not isinstance(entity, (UnconfiguredDeviceEntity, UnsupportedDeviceEntity)):
                logger.debug("Removing old entity before re-discovery: %s", entity.unique_id)
                await entity.remove_config(self.app.mqtt_manager)
                Entity.remove_entity(entity.unique_id)

        # --- Entity Discovery Logic ---
        device_type = node.device_info.get("type")
        if device_type == "bluepill_relay":
            logger.info("Device is a bluepill_relay. Creating 8 SimpleLight entities.")
            for i in range(8):
                entity_index = i + 1
                # Create a SimpleLight entity. TypeID 5, Version 0
                entity = EntityRegistry.create(5, node, entity_index, self.app.mqtt_topic_prefix, 0)
                entity.props["name"] = f"Relay {entity_index}"
                # The state comes from a shared TPDO, so we only need the command SDO
                entity.setup_object_dictionary(node, 0x2000 + entity_index * 16)
                await entity.publish_config(self.app.mqtt_manager)
        else:
            # Fallback to original can2mqtt logic for other devices
            await self.discover_entities_from_sdo(node)

        # Re-read and apply TPDO configuration
        logger.debug("Reading TPDO configuration for node %02x", node.id)
        await node.tpdo.aread()
        on_tpdo = self.app.can_manager.get_tpdo_cb()
        for map_ in node.tpdo.map.values():
            map_.add_callback(on_tpdo)

    async def discover_entities_from_sdo(self, node):
        """Fallback entity discovery compatible with original can2mqtt."""
        logger.info("Using fallback SDO discovery for node %02x", node.id)
        # This logic is similar to the original can2mqtt
        entity_types_index = 0x2001
        async for entity_index, entity_type in async_try_iter_items(node.sdo[entity_types_index]):
            try:
                entity = EntityRegistry.create(entity_type, node, entity_index, self.app.mqtt_topic_prefix)
                logger.info("  Discovered entity: %r", entity)
                base_index = 0x2000 + entity_index * 16
                entity.setup_object_dictionary(node, base_index)
                async for key, value in async_try_iter_items(node.sdo[base_index]):
                    entity.set_metadata_property(key, value)
                await entity.publish_config(self.app.mqtt_manager)
            except KeyError:
                logger.warning("  Unknown entity type %d at index %d on node %02x", entity_type, entity_index, node.id)
