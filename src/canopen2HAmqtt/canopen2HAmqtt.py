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
from .entities import Entity, EntityRegistry, StateMixin, CommandMixin, UnconfiguredDeviceEntity, UnsupportedDeviceEntity, SimpleLight

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


async def ensure_can_interface_up(device: str, bitrate: str):
    """
    Uses the 'ip' command to configure and bring up the CAN interface.
    This function attempts to detect if it's in a full iproute2 environment or a
    limited BusyBox environment and adapts its strategy accordingly.
    """
    logging.info(f"Attempting to configure CAN interface '{device}'...")

    # First, try to use `ip -details` which is only available in iproute2
    is_iproute2 = False
    is_can_device = False
    try:
        proc_check = await asyncio.create_subprocess_shell(
            f"ip -details link show {device}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_bytes, stderr_bytes = await proc_check.communicate()
        stderr = stderr_bytes.decode().strip()

        if proc_check.returncode == 0:
            is_iproute2 = True
            is_can_device = "link/can" in stdout_bytes.decode().strip()
            logging.info(f"Detected iproute2 environment. Interface '{device}' is a can device: {is_can_device}")
        else:
            # Check for errors that indicate a BusyBox environment
            if "invalid option" in stderr or "Usage: ip" in stderr:
                logging.warning("`ip -details` not supported, falling back to BusyBox-compatible mode.")
            else: # Another error occurred, might not be a can device yet
                logging.info(f"'{device}' may not exist yet, proceeding with creation.")

    except FileNotFoundError:
        logging.error("The 'ip' command is not found. This is required to configure the CAN interface.")
        raise

    # --- Configuration sequence ---
    try:
        # 1. Bring interface down (best-effort to allow bitrate changes).
        cmd_down = f"ip link set {device} down"
        logging.debug(f"Executing: {cmd_down}")
        proc_down = await asyncio.create_subprocess_shell(cmd_down, stderr=asyncio.subprocess.PIPE)
        _, stderr_down_bytes = await proc_down.communicate()
        if proc_down.returncode != 0:
            error_msg = stderr_down_bytes.decode().strip()
            if "No such device" not in error_msg:
                logging.warning(f"Could not bring interface '{device}' down (it may be down already): {error_msg}")

        # 2. Set CAN type and bitrate.
        # If we know it's already a can device, don't set the type again.
        if is_iproute2 and is_can_device:
            cmd_set = f"ip link set {device} bitrate {bitrate}"
            logging.info(f"Setting bitrate for existing CAN interface '{device}'...")
        else:
            cmd_set = f"ip link set {device} type can bitrate {bitrate}"
            logging.info(f"Setting CAN type and bitrate for '{device}'...")

        proc_set = await asyncio.create_subprocess_shell(cmd_set, stderr=asyncio.subprocess.PIPE)
        _, stderr_set_bytes = await proc_set.communicate()
        stderr_set = stderr_set_bytes.decode().strip()

        if proc_set.returncode != 0:
            # Handle common "already configured" errors gracefully
            if "RTNETLINK answers: File exists" in stderr_set or "either \"dev\" is duplicate" in stderr_set:
                logging.warning(f"Interface '{device}' likely already configured. Ignoring error: {stderr_set}")
            elif "Device or resource busy" in stderr_set:
                 logging.warning(f"Could not set type/bitrate for '{device}' (device is busy). Ignoring error: {stderr_set}")
            else:
                # Any other error is a real problem.
                raise RuntimeError(f"Failed to configure CAN interface '{device}': {stderr_set}")

        # 3. Bring interface up.
        cmd_up = f"ip link set {device} up"
        logging.debug(f"Executing: {cmd_up}")
        proc_up = await asyncio.create_subprocess_shell(cmd_up, stderr=asyncio.subprocess.PIPE)
        _, stderr_up_bytes = await proc_up.communicate()
        if proc_up.returncode != 0:
            error_msg = stderr_up_bytes.decode().strip()
            # If it's already up, "File exists" or "Device or resource busy" can be returned.
            if "Device or resource busy" in error_msg or "RTNETLINK answers: File exists" in error_msg:
                 logging.warning(f"Could not bring up CAN interface '{device}' (may be already up). Ignoring error: {error_msg}")
            else:
                raise RuntimeError(f"Failed to bring up CAN interface '{device}': {error_msg}")

        logging.info(f"CAN interface '{device}' configuration sequence completed successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during CAN interface setup: {e}")
        raise


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
        # A status of 0 is a boot-up message
        if status == 0 and node.is_initialized:
            logger.info(f"Node {node.id} has sent a boot-up message. Checking for firmware changes.")
            
            async def re_discover_check():
                """Asynchronously check identity and re-discover if changed."""
                try:
                    identity = await asyncio.gather(
                        node.sdo["Identity"]["VendorId"].aget_raw(),
                        node.sdo["Identity"]["ProductCode"].aget_raw(),
                        node.sdo["SoftwareVersion"].aget_raw()
                    )
                    new_vendor_id, new_product_code, new_sw_version = identity
                    
                    # Compare with stored info
                    if (node.device_info.get('vendor_id') != new_vendor_id or
                        node.device_info.get('product_code') != new_product_code or
                        node.sw_version != new_sw_version):
                        logger.info(f"Firmware change detected for node {node.id}. Triggering re-discovery.")
                        node.is_initialized = False
                    else:
                        logger.info(f"Node {node.id} reset, but firmware is unchanged.")
                except Exception as e:
                    logger.error(f"Error during re-discovery check for node {node.id}: {e}")

            asyncio.create_task(re_discover_check())

    return on_heartbeat


def get_tpdo_cb(mqtt_client):
    """Callback for CANopen TPDO messages."""
    async def on_tpdo(map):
        node_id = map.pdo_node.node.id
        for var in map: # var is an ODVariable that was mapped
            # Check if the updated variable is the relay state mask from our new SDO
            if var.index == 0x2100 and var.subindex == 0:
                try:
                    state_mask = await var.aget_raw()
                    logger.debug(f"Received TPDO with relay state mask for node {node_id}: {state_mask:08b}")
                    
                    # Update all 8 light entities based on the new mask
                    for i in range(8):
                        # The entity_index in the addon is 1-based, relay index is 0-based
                        entity_unique_id = f"can_{node_id:03x}_{(i + 1):02x}"
                        entity = Entity.get_entity_by_unique_id(entity_unique_id)
                        
                        if entity and isinstance(entity, SimpleLight):
                            individual_state = (state_mask >> i) & 1
                            if entity.state_map:
                                state_key = entity.state_map[0] # SimpleLight has only one state
                                topic, mqtt_value = entity.get_mqtt_state(state_key, individual_state)
                                await mqtt_client.publish(topic, payload=mqtt_value, retain=False)
                                logger.debug(f"Updated {entity.unique_id} to {mqtt_value} from TPDO mask.")
                except Exception as e:
                    logger.error(f"Error processing TPDO bitmask for node {node_id}: {e}")
                return # We handled the bitmask, so we can stop processing this PDO map
            
            # Fallback for any other potential 1-to-1 PDO mappings
            else:
                try:
                    key = (var.index << 16) | (var.subindex << 8)
                    entity = StateMixin.get_entity_by_node_state_key(node_id, key)
                    if entity:
                        state_topic, value = entity.get_mqtt_state(key, await var.aget_raw())
                        await mqtt_client.publish(state_topic, payload=value, retain=False)
                        logger.debug("MQTT TPDO (fallback) publish topic: %s value: %s", state_topic, value)
                except ValueError as e:
                    logger.error("Error publishing fallback TPDO state for node %d: %s", node_id, e)

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
        # Overwrite default values with ones from the device
        node.sw_version = await node.sdo["SoftwareVersion"].aget_raw()
        node.device_name = await node.sdo[0x1008].aget_raw()
        node.hw_version = await node.sdo[0x1009].aget_raw()
    except (SdoAbortedError, SdoCommunicationError) as e:
        logger.warning("Node %02x: Could not read one or more basic info properties (SW/HW Version/Device Name). Using defaults. Error: %s", node.id, e)
        pass # Defaults are already set during node initialization
    
    node.is_reconfiguring = False

    # Remove existing entities before re-discovery
    for entity in list(Entity.entities()):
        if entity.node.id == node.id and not isinstance(entity, (UnconfiguredDeviceEntity, UnsupportedDeviceEntity)):
            logger.debug("Removing old entity before re-discovery: %s", entity.unique_id)
            await entity.remove_config(mqtt_client)
            Entity.remove_entity(entity.unique_id)

    # Discover and publish config for all entities from SDO 0x2001
    entity_types_index = 0x2001
    if entity_types_index not in node.object_dictionary:
        arr = ODArray("EntityTypes", entity_types_index)
        arr.add_member(od_variable(datatypes.UNSIGNED8, "len", entity_types_index, 0))
        arr.add_member(od_variable(datatypes.UNSIGNED32, "item1", entity_types_index, 1))
        node.object_dictionary.add_object(arr)

    async for entity_index, entity_type in async_try_iter_items(node.sdo[entity_types_index]):
        try:
            entity = EntityRegistry.create(entity_type, node, entity_index, mqtt_topic_prefix)
            logger.info("  Discovered entity: %r", entity)
        except KeyError:
            logger.warning("  Unknown entity type %d at index %d on node %02x", entity_type, entity_index, node.id)
            continue

        base_index = 0x2000 + entity_index * 16
        node.object_dictionary.add_object(ODRecord("states", base_index + 1))
        node.object_dictionary.add_object(ODRecord("cmds", base_index + 2))
        entity.setup_object_dictionary(node, base_index)
        async for key, value in async_try_iter_items(node.sdo[base_index]):
            entity.set_metadata_property(key, value)
        await entity.publish_config(mqtt_client)
    
    # Read initial state from the master mask (0x2100) and publish to all entities
    try:
        state_mask = await node.sdo[0x2100].aget_raw()
        logger.info(f"Read initial relay state mask for node {node.id}: {state_mask:08b}")

        for i in range(8):
            entity_unique_id = f"can_{node.id:03x}_{(i + 1):02x}"
            entity = Entity.get_entity_by_unique_id(entity_unique_id)
            
            if entity and isinstance(entity, SimpleLight) and entity.state_map:
                individual_state = (state_mask >> i) & 1
                state_key = entity.state_map[0]
                topic, mqtt_value = entity.get_mqtt_state(state_key, individual_state)
                await mqtt_client.publish(topic, payload=mqtt_value, retain=False)
                logger.debug(f"Published initial state for {entity.unique_id}: {mqtt_value}")

    except Exception as e:
        logger.error(f"Could not read initial state mask from 0x2100 for node {node.id}: {e}")

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
        node.device_name = f"CANopen Node {node.id}"
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
        if not hasattr(node, 'has_nmt_callback') or not node.has_nmt_callback:
            node.nmt.add_heartbeat_callback(get_heartbeat_cb(mqtt_client, node))
            node.has_nmt_callback = True
    except (SdoAbortedError, SdoCommunicationError):
        logger.warning("Node %02x: Could not read ProducerHeartbeatTime. Availability monitoring may be impaired.", node.id)

    try:
        device_name_from_node = await node.sdo[0x1008].aget_raw()
        if not device_name_from_node.strip():
            raise SdoAbortedError("Device name is empty")
        node.device_name = device_name_from_node
        logger.info("Node %02x is already configured as '%s'. Processing entities.", node.id, node.device_name)
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

    generic_od = import_od(os.path.join(BASE_DIR, "eds/bluepill.eds"))
    unsupported_nodes = set()

    while True:
        watchdog.reset()
        await asyncio.sleep(1.0)
        
        # --- Handle Node 0 (unconfigured) ---
        if UNCONFIGURED_NODE_ID in can_network.scanner.nodes and not can_network.get(UNCONFIGURED_NODE_ID):
            logger.info("Unconfigured device (node ID 0) detected.")
            temp_node_0 = can_network.add_node(UNCONFIGURED_NODE_ID, generic_od)
            temp_node_0.sdo.RESPONSE_TIMEOUT = sdo_timeout
            try:
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
            continue

        # --- Handle known and new nodes ---
        nodes_to_process = set(can_network.scanner.nodes)
        for node_id in nodes_to_process:
            if node_id in unsupported_nodes or node_id == UNCONFIGURED_NODE_ID:
                continue

            node = can_network.get(node_id)
            if not node: # New node detected
                logger.info("New node %02x detected. Trying to identify...", node_id)
                temp_node = can_network.add_node(node_id, generic_od)
                temp_node.sdo.RESPONSE_TIMEOUT = sdo_timeout
                try:
                    vendor_id = await temp_node.sdo[0x1018][1].aget_raw()
                    product_code = await temp_node.sdo[0x1018][2].aget_raw()
                except (SdoCommunicationError, SdoAbortedError) as e:
                    # Smart filter for bogus nodes from non-standard PDOs
                    is_likely_bogus_pdo = False
                    for known_node in can_network.values():
                        if getattr(known_node, 'is_supported', False):
                            # Heuristic: a real node won't be in the TPDO1-4 COB-ID range
                            if known_node.id < node_id < known_node.id + 16:
                                logger.warning(f"Ignoring discovery of node {node_id}, as it is likely a non-standard PDO from supported node {known_node.id}.")
                                is_likely_bogus_pdo = True
                                break
                    if not is_likely_bogus_pdo:
                         logger.error("Failed to query identity of new node %02x: %s", node_id, e)
                    
                    unsupported_nodes.add(node_id)
                    del can_network[node_id]
                    continue
                
                device_info = next((d for d in devices_config if d['vendor_id'] == vendor_id and d['product_code'] == product_code), None)
                if device_info and 'eds_file' in device_info:
                    del can_network[node_id]
                    od = import_od(os.path.join(BASE_DIR, "eds", device_info['eds_file']))
                    logger.info("Loading EDS '%s' for node %02x", device_info['eds_file'], node_id)
                    node = can_network.add_node(node_id, od)

                    # Ensure the RelayStateMask object (0x2100) exists, for robustness
                    if 0x2100 not in node.object_dictionary:
                        logging.info("Object 0x2100 (RelayStateMask) not found in EDS, adding it programmatically.")
                        relay_state_mask = ODVariable("RelayStateMask", 0x2100, 0)
                        relay_state_mask.data_type = datatypes.UNSIGNED8
                        relay_state_mask.access_type = 'ro'
                        node.object_dictionary.add_object(relay_state_mask)
                else:
                    logger.warning("No matching device config for node %02x (Vendor: %s, Product: %s). Creating informational entity.", node_id, hex(vendor_id), hex(product_code))
                    unsupported_entity = EntityRegistry.create(253, temp_node, 0, mqtt_topic_prefix, 0)
                    unsupported_entity.vendor_id = vendor_id
                    unsupported_entity.product_code = product_code
                    await unsupported_entity.publish_config(mqtt_client)
                    await unsupported_entity.mqtt_initial_publish(mqtt_client)
                    unsupported_nodes.add(node_id)
                    del can_network[node_id]
                    continue

                node.sdo.RESPONSE_TIMEOUT = sdo_timeout
                node.is_initialized = False
                node.is_supported = False
                node.last_heartbeat_time = time.time()
                node.availability = None
                node.availability_topic = f"{mqtt_topic_prefix}/can_{node_id:03x}/availability"
                node.prod_heartbeat_time = None
                node.has_nmt_callback = False
                node.watchdog = WatchdogTimer(None)
                node.is_reconfiguring = False
                node.device_info = {}
                node.device_name = f"CANopen Node {node.id}" # Default name
                node.sw_version = "N/A"
                node.hw_version = ""

            just_registered = False
            if not node.is_initialized:
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


async def start(mqtt_server, interface, channel, bitrate, mqtt_topic_prefix, sdo_response_timeout=0.5, watchdog_timeout=60, devices=None, configure_can_interface=False, **kwargs):
    main_watchdog = WatchdogTimer(watchdog_timeout)
    mqtt_host, auth = parse_mqtt_server_url(mqtt_server)
    will = aiomqtt.Will(f"{mqtt_topic_prefix}/canopen2HAmqtt/status", b"offline", 1, retain=True)
    
    can_network = canopen.Network()
    
    logger.info("Connecting to MQTT server at %s", mqtt_host)
    async with aiomqtt.Client(mqtt_host, will=will, **auth) as mqtt_client:
        try:
            if interface == 'socketcan' and configure_can_interface:
                # Configure SocketCAN interface using its device name (e.g., can0) from 'channel'
                await ensure_can_interface_up(channel, str(bitrate))

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
            logger.exception("Failed to initialize and start the addon: %s", e)
        finally:
            logger.info("Disconnecting and publishing offline status...")
            for node in can_network.values():
                if hasattr(node, 'is_supported') and node.is_supported:
                    await mqtt_client.publish(node.availability_topic, payload="offline", retain=True)
            await publish_addon_status(mqtt_client, mqtt_topic_prefix, "offline")
            can_network.disconnect()
            logger.info("Shutdown complete.")
    return 0
