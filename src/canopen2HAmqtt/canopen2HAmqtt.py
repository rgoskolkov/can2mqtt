import asyncio
import logging
import json
import os
import re
import time

import aiomqtt
import canopen
from canopen.network import Network
from canopen.node import RemoteNode
from canopen.sdo.exceptions import SdoAbortedError, SdoCommunicationError
from canopen.objectdictionary import import_od, datatypes, ODRecord, ODArray, ODVariable

from .utils import parse_mqtt_server_url
from .entities import EntityRegistry, StateMixin, CommandMixin, Entity, UnconfiguredDeviceEntity

# SDO Abort Codes
CODE_SUBINDEX_NOT_FOUND = 0x06090011
CODE_OBJECT_NOT_FOUND = 0x06020000

UNCONFIGURED_NODE_ID = 0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)


class QuitException(Exception):
    def __init__(self, descr, exit_code=1):
        super().__init__(descr)
        self.exit_code = exit_code


async def async_try_iter_items(obj):
    """Safely iterate over an SDO array, skipping non-existent subindexes."""
    try:
        async for key in obj:
            if not key:
                continue
            try:
                val = await obj[key].aget_raw()
                yield key, val
            except SdoAbortedError as e:
                if e.code == CODE_SUBINDEX_NOT_FOUND:
                    logger.debug("Subindex %s not found in SDO object, skipping.", key)
                    continue
                raise
    except SdoAbortedError as e:
        if e.code != CODE_OBJECT_NOT_FOUND:
            raise


class WatchdogTimer:
    def __init__(self, timeout):
        self._timeout = timeout
        self._time = time.time()

    def reset(self):
        self._time = time.time()

    def passed(self):
        return time.time() - self._time >= self._timeout if self._timeout else False


def get_heartbeat_cb(mqtt_client, node):
    """Callback for CANopen heartbeat messages."""
    def on_heartbeat(status):
        node.watchdog.reset()
        logger.debug("Heartbeat from %02x: %s", node.id, status)
        node.last_heartbeat_time = time.time()
        if node.ntm_state_entity:
            state_topic = node.ntm_state_entity.get_state_topic()
            asyncio.create_task(mqtt_client.publish(state_topic, payload=str(status), retain=False))
    return on_heartbeat


def get_tpdo_cb(mqtt_client):
    """Callback for CANopen TPDO messages."""
    async def on_tpdo(map):
        node_id = map.pdo_node.node.id
        for v in map.map:
            key = (v.index << 16) | (v.subindex << 8)
            entity = StateMixin.get_entity_by_node_state_key(node_id, key)
            if entity:
                try:
                    state_topic, value = entity.get_mqtt_state(key, await v.aget_raw())
                    await mqtt_client.publish(state_topic, payload=value, retain=False)
                    logger.debug("MQTT TPDO publish topic: %s value: %s", state_topic, value)
                except ValueError as e:
                    logger.error("Error publishing TPDO state for node %d: %s", node_id, e)
            else:
                logger.warning("No entity found for TPDO from node: %d, key: %08x", node_id, key)
    return on_tpdo


def od_variable(data_type, name, index, subindex, default=None):
    """Helper to create an ODVariable."""
    var = ODVariable(name, index, subindex)
    var.data_type = data_type
    if default is not None:
        var.default = default
    return var


async def process_node_entities(mqtt_client, mqtt_topic_prefix: str, node: RemoteNode):
    """Reads entity definitions from a device and creates/updates them in Home Assistant."""
    logger.info("Processing entities for node %02x...", node.id)
    try:
        node.sw_version = await node.sdo["SoftwareVersion"].aget_raw()
        node.device_name = await node.sdo[0x1008].aget_raw()
    except (SdoAbortedError, SdoCommunicationError) as e:
        logger.warning("Node %02x: Could not read basic info (SW Version/Device Name): %s", node.id, e)
        node.sw_version = node.sw_version or "N/A"
        node.device_name = node.device_name or f"CANopen Node {node.id}"
    
    node.is_reconfiguring = False

    # Remove existing entities before re-discovery, except persistent ones
    for entity in list(Entity.entities()):
        if entity.node.id == node.id and not isinstance(entity, (NMTStateSensor, UnconfiguredDeviceEntity)):
            logger.debug("Removing old entity before re-discovery: %s", entity.unique_id)
            await entity.remove_config(mqtt_client)
            Entity.remove_entity(entity.unique_id)

    # Entity types are at index 0x2001 (UNSIGNED32)
    entity_types_index = 0x2001
    if entity_types_index not in node.object_dictionary:
        arr = ODArray("EntityTypes", entity_types_index)
        arr.add_member(od_variable(datatypes.UNSIGNED8, "len", entity_types_index, 0))
        arr.add_member(od_variable(datatypes.UNSIGNED32, "item1", entity_types_index, 1))
        node.object_dictionary.add_object(arr)

    node_entity_ids = set()
    async for entity_index, entity_type in async_try_iter_items(node.sdo[entity_types_index]):
        try:
            entity = EntityRegistry.create(entity_type, node, entity_index, mqtt_topic_prefix)
            node_entity_ids.add(entity.unique_id)
            logger.info("  Discovered entity: %r", entity)
        except KeyError:
            logger.warning("  Unknown entity type %d at index %d on node %02x", entity_type, entity_index, node.id)
            continue

        base_index = 0x2000 + entity_index * 16
        entity.setup_object_dictionary(node, base_index)
        async for key, value in async_try_iter_items(node.sdo[base_index]):
            entity.set_metadata_property(key, value)
        await entity.publish_config(mqtt_client)

    # Publish initial state for newly discovered entities
    for entity_id in node_entity_ids:
        entity = Entity._entities.get(entity_id)
        if entity:
            await entity.mqtt_initial_publish(mqtt_client)

    # Re-read and apply TPDO configuration
    logger.debug("Re-reading TPDO configuration for node %02x", node.id)
    for map in node.tpdo.map.values():
        map.clear()
    await node.tpdo.aread()
    on_tpdo = get_tpdo_cb(mqtt_client)
    for map in node.tpdo.map.values():
        map.add_callback(on_tpdo)


async def register_new_node(mqtt_client, mqtt_topic_prefix, can_network, node, devices_config):
    """Handles a newly detected or re-initialized node on the bus."""
    try:
        identity = await asyncio.gather(
            node.sdo["Identity"]["VendorId"].aget_raw(),
            node.sdo["Identity"]["ProductCode"].aget_raw(),
        )
        vendor_id, product_code = identity
    except (SdoCommunicationError, SdoAbortedError) as e:
        logger.warning("Node %02x: Could not read identity, skipping. Error: %s", node.id, e)
        return

    device_info = next((d for d in devices_config if d['vendor_id'] == vendor_id and d['product_code'] == product_code), None)
    if not device_info:
        logger.warning("Node %02x (Vendor: %s, Product: %s) is not supported, skipping.", node.id, hex(vendor_id), hex(product_code))
        return
    
    node.device_info = device_info
    node.is_supported = True
    logger.info("Registering supported node %02x: %s", node.id, device_info.get("name", "Unnamed Device"))
    
    try:
        hb_time = await node.sdo["ProducerHeartbeatTime"].aget_raw()
        node.prod_heartbeat_time = hb_time
        node.watchdog = WatchdogTimer(2 * hb_time / 1000.0 if hb_time else None)
        if not node.has_nmt_callback:
            node.nmt.add_hearbeat_callback(get_heartbeat_cb(mqtt_client, node))
            node.has_nmt_callback = True
    except (SdoAbortedError, SdoCommunicationError):
        logger.warning("Node %02x: Could not read ProducerHeartbeatTime. Availability monitoring may be impaired.", node.id)

    # Create NMT state sensor
    if not node.ntm_state_entity:
        nmt_entity = EntityRegistry.create(0, node, 0, mqtt_topic_prefix)
        nmt_entity.set_property("name", "NMT State")
        node.ntm_state_entity = nmt_entity
        await nmt_entity.publish_config(mqtt_client)

    # Check if device is configured by reading its name. If not, publish config entity.
    try:
        device_name = await node.sdo[0x1008].aget_raw()
        if not device_name.strip():
            raise SdoAbortedError("Device name is empty")
        logger.info("Node %02x is already configured as '%s'. Processing entities.", node.id, device_name)
        await process_node_entities(mqtt_client, mqtt_topic_prefix, node)
    except (SdoAbortedError, SdoCommunicationError):
        logger.info("Node %02x is unconfigured. Publishing configuration entity.", node.id)
        config_entity = EntityRegistry.create(254, node, 254, mqtt_topic_prefix)
        config_entity.props['name'] = f"Unconfigured Device (Node {node.id})"
        await config_entity.publish_config(mqtt_client)
        node.is_reconfiguring = True
    
    node.is_initialized = True


async def find_free_node_id(can_network: Network) -> int:
    """Finds an available node ID by checking for active nodes."""
    used_ids = set(can_network.scanner.nodes)
    for node_id in range(1, 128):
        if node_id not in used_ids:
            return node_id
    raise Exception("No free node ID found (1-127).")


async def can_bus_reader(can_network, mqtt_client, mqtt_topic_prefix, devices_config, watchdog, sdo_timeout):
    """Periodically checks for new and existing nodes on the bus."""
    await publish_addon_status(mqtt_client, mqtt_topic_prefix, "online")

    # Generic OD for initial communication with unconfigured nodes
    generic_od = import_od(os.path.join(BASE_DIR, "eds/bluepill.eds"))

    while True:
        watchdog.reset()

        # Give the network some time to process incoming messages from the background scanner
        await asyncio.sleep(1.0)
        
        # --- Handle unconfigured devices (Node ID 0) first ---
        # The canopen-async library populates scanner.nodes in the background
        if UNCONFIGURED_NODE_ID in can_network.scanner.nodes and not can_network.get(UNCONFIGURED_NODE_ID):
            logger.info("Unconfigured device (node ID 0) detected.")
            temp_node_0 = can_network.add_node(UNCONFIGURED_NODE_ID, generic_od)
            temp_node_0.sdo.RESPONSE_TIMEOUT = sdo_timeout
            try:
                # Find a free ID that is not already on the bus
                all_node_ids = set(can_network.scanner.nodes) | {node.id for node in can_network.values()}
                free_node_id = 1
                while free_node_id in all_node_ids:
                    free_node_id += 1
                if free_node_id > 127:
                    raise Exception("No free node ID found (1-127).")

                logger.info("Assigning new node ID %d to unconfigured device.", free_node_id)
                await temp_node_0.sdo[0x2002].aset_raw(free_node_id)
                await temp_node_0.nmt.state_set("RESET NODE")
                logger.info("Device reset. Waiting for it to reappear with new ID %d.", free_node_id)
            except Exception as e:
                logger.error("Failed to configure new device from ID 0: %s", e)
            finally:
                if UNCONFIGURED_NODE_ID in can_network:
                    del can_network[UNCONFIGURED_NODE_ID]
            continue # Restart the loop to handle the newly numbered node

        # --- Handle all other known and unknown nodes ---
        for node_id in can_network.scanner.nodes:
            if node_id == UNCONFIGURED_NODE_ID:
                continue

            node = can_network.get(node_id)
            if not node:
                # New node detected, add it to our network object
                logger.info("New node %02x detected. Trying to identify...", node_id)
                # Temporarily add with generic OD to read identity
                temp_node = can_network.add_node(node_id, generic_od)
                temp_node.sdo.RESPONSE_TIMEOUT = sdo_timeout
                try:
                    vendor_id = await temp_node.sdo[0x1018][1].aget_raw()
                    product_code = await temp_node.sdo[0x1018][2].aget_raw()
                except (SdoCommunicationError, SdoAbortedError) as e:
                    logger.error("Failed to query identity of new node %02x: %s", node_id, e)
                    del can_network[node_id]
                    continue
                finally:
                    # remove temp node before adding real one
                    if node_id in can_network:
                        del can_network[node_id]

                device_info = next((d for d in devices_config if d['vendor_id'] == vendor_id and d['product_code'] == product_code), None)
                if device_info and 'eds_file' in device_info:
                    od = import_od(os.path.join(BASE_DIR, "eds", device_info['eds_file']))
                    logger.info("Loading EDS '%s' for node %02x", device_info['eds_file'], node_id)
                else:
                    logger.warning("No matching device config for node %02x (Vendor: %s, Product: %s), skipping.", node_id, hex(vendor_id), hex(product_code))
                    continue # Skip unsupported device

                node = can_network.add_node(node_id, od)
                node.sdo.RESPONSE_TIMEOUT = sdo_timeout
                node.is_initialized = False
                node.is_supported = False # Will be set in register_new_node
                node.last_heartbeat_time = time.time()
                node.availability = None
                node.availability_topic = f"{mqtt_topic_prefix}/can_{node_id:03x}/availability"
                node.prod_heartbeat_time = None
                node.ntm_state_entity = None
                node.has_nmt_callback = False
                node.watchdog = WatchdogTimer(None) # Will be updated from heartbeat time
                node.is_reconfiguring = False
                node.device_info = {}

            just_registered = False
            if not node.is_initialized and node.nmt.state == "OPERATIONAL":
                try:
                    logger.info("Registering node: %02x", node.id)
                    await register_new_node(mqtt_client, mqtt_topic_prefix, can_network, node, devices_config)
                    just_registered = True
                except (SdoCommunicationError, SdoAbortedError) as e:
                    logger.warning("Could not register node %02x: %s", node.id, e)
                except Exception:
                    logger.exception("Unknown exception while registering node %02x", node.id)

            if node.is_supported and not node.is_reconfiguring:
                is_online = not node.prod_heartbeat_time or ((time.time() - node.last_heartbeat_time) < (2 * node.prod_heartbeat_time / 1000.0))
                availability = "online" if is_online else "offline"
                if node.availability != availability or just_registered:
                    logger.info("Node %02x is now %s", node_id, availability)
                    node.availability = availability
                    await mqtt_client.publish(node.availability_topic, payload=node.availability, retain=True)
        
        if watchdog.passed():
            raise QuitException("Main watchdog timeout")


async def mqtt_message_reader(mqtt_client, can_network, mqtt_topic_prefix):
    """Listens for and processes incoming MQTT messages."""
    async with mqtt_client.messages() as messages:
        await mqtt_client.subscribe(f"{mqtt_topic_prefix}/#")
        async for message in messages:
            topic = message.topic.value
            logger.debug("Received MQTT message on topic '%s' with payload: %s", topic, message.payload[:80])

            if topic == f"{mqtt_topic_prefix}/status" and message.payload == b"online":
                logger.info("HA requested status update. Re-publishing all configs and states.")
                for entity in Entity.entities():
                    await entity.publish_config(mqtt_client)
                    await entity.mqtt_initial_publish(mqtt_client)
                for node in can_network.values():
                    if node and node.is_supported:
                        await mqtt_client.publish(node.availability_topic, payload="online", retain=True)
                await publish_addon_status(mqtt_client, mqtt_topic_prefix, "online")
                continue

            entity = CommandMixin.get_entity_by_cmd_topic(topic)
            if not entity:
                continue

            if isinstance(entity, UnconfiguredDeviceEntity):
                try:
                    config_str = message.payload.decode('utf-8').strip()
                    if not config_str: continue
                    device_name = config_str.split(',')[0].strip()
                    logger.info("Applying configuration to node %02x: set name to '%s'", entity.node.id, device_name)
                    await entity.node.sdo[0x1008].aset_raw(device_name.encode('utf-8'))
                    await process_node_entities(mqtt_client, mqtt_topic_prefix, entity.node)
                    await entity.remove_config(mqtt_client)
                    Entity.remove_entity(entity.unique_id)
                except Exception as e:
                    logger.error("Failed to apply configuration for node %02x: %s", entity.node.id, e)
            else:
                try:
                    cmd_key, value = entity.get_can_cmd(topic, message.payload)
                    var = entity.node.sdo[cmd_key >> 16][(cmd_key >> 8) & 0xFF]
                    await var.aset_raw(value)
                    logger.debug("Sent command to %r: key=%08x, value=%s", entity, cmd_key, value)
                except Exception as e:
                    logger.error("Error processing command for %r: %s", entity, e)


async def publish_addon_status(mqtt_client, mqtt_topic_prefix, status):
    status_topic = f"{mqtt_topic_prefix}/canopen2HAmqtt/status"
    await mqtt_client.publish(status_topic, payload=status, retain=True)


async def start(mqtt_server, interface, channel, bitrate, mqtt_topic_prefix, sdo_response_timeout=0.5, watchdog_timeout=60, devices=None, **kwargs):
    main_watchdog = WatchdogTimer(watchdog_timeout)
    mqtt_host, auth = parse_mqtt_server_url(mqtt_server)
    will = aiomqtt.Will(f"{mqtt_topic_prefix}/canopen2HAmqtt/status", b"offline", 1, retain=True)
    
    logger.info("Connecting to MQTT server at %s", mqtt_host)
    async with aiomqtt.Client(mqtt_host, will=will, **auth) as mqtt_client:
        can_network = canopen.Network()
        try:
            can_network.connect(loop=asyncio.get_running_loop(), interface=interface, channel=channel, bitrate=bitrate, **kwargs)
            logger.info(f"Connected to CAN bus via {interface}:{channel} at {bitrate} bps")
            await asyncio.gather(
                can_bus_reader(can_network, mqtt_client, mqtt_topic_prefix, devices, main_watchdog, sdo_response_timeout),
                mqtt_message_reader(mqtt_client, can_network, mqtt_topic_prefix),
            )
        except QuitException as e:
            logger.warning("Application quitting: %s", e)
            return e.exit_code
        except Exception as e:
            logger.exception("An unhandled exception occurred in main loop: %s", e)
        finally:
            logger.info("Disconnecting and publishing offline status...")
            for node in can_network.values():
                if hasattr(node, 'is_supported') and node.is_supported:
                    await mqtt_client.publish(node.availability_topic, payload="offline", retain=True)
            await publish_addon_status(mqtt_client, mqtt_topic_prefix, "offline")
            can_network.disconnect()
            logger.info("Shutdown complete.")
    return 0
