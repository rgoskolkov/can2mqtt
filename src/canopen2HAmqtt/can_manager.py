import asyncio
import logging
import os
import time

import canopen
from canopen.sdo.exceptions import SdoAbortedError, SdoCommunicationError
from canopen.objectdictionary import import_od
from .app import CanOpen2HAmqtt, QuitException

from .entities import Entity, EntityRegistry, SimpleLight, StateMixin, UnconfiguredDeviceEntity, UnsupportedDeviceEntity

logger = logging.getLogger(__name__)
UNCONFIGURED_NODE_ID = 0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class CanManager:
    app: CanOpen2HAmqtt
    def __init__(self, app):
        self.app = app
        self.can_network = canopen.Network()
        self.unsupported_nodes = set()
        # Pass kwargs for can_network.connect from the main start function
        self.connect_kwargs = app.kwargs 

    def get_nodes(self):
        return self.can_network.values()

    def shutdown(self):
        if self.can_network.bus:
            self.can_network.disconnect()

    async def start(self):
        """
        Initializes the CAN interface, connects to the network, and starts background tasks.
        This is the single entry point for all CAN-related operations.
        """
        if self.app.interface == 'socketcan' and self.app.configure_can_interface:
            await self._ensure_can_interface_up(self.app.channel, str(self.app.bitrate))

        self.can_network.connect(
            loop=asyncio.get_running_loop(),
            interface=self.app.interface,
            channel=self.app.channel,
            bitrate=self.app.bitrate,
            **self.connect_kwargs
        )
        logger.info(f"Connected to CAN bus via {self.app.interface}:{self.app.channel} at {self.app.bitrate} bps")

        self.can_network.scanner.add_callback(self.on_node_detected)
        self.can_network.scanner.start()

        # Run periodic availability checks as a long-running task
        await self._check_node_availability()

    async def _ensure_can_interface_up(self, device: str, bitrate: str):
        """Uses the 'ip' command to configure and bring up the CAN interface."""
        logging.info(f"Attempting to configure CAN interface '{device}'...")
        try:
            await asyncio.create_subprocess_shell(f"ip link set {device} down")
            proc_set = await asyncio.create_subprocess_shell(
                f"ip link set {device} type can bitrate {bitrate}",
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr_bytes = await proc_set.communicate()
            if proc_set.returncode != 0 and "File exists" not in stderr_bytes.decode():
                raise RuntimeError(f"Failed to set CAN type and bitrate for '{device}': {stderr_bytes.decode()}")
            await asyncio.create_subprocess_shell(f"ip link set {device} up")
            logging.info(f"CAN interface '{device}' configured successfully.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during CAN interface setup: {e}")
            raise

    async def on_node_detected(self, node_id):
        """Callback for when a new node is detected on the bus (initialization phase per node)."""
        if node_id in self.can_network or node_id in self.unsupported_nodes:
            return

        logger.info("New node %02x detected. Trying to identify...", node_id)
        
        if node_id == UNCONFIGURED_NODE_ID:
            await self.handle_unconfigured_node_zero()
            return

        temp_node = self.can_network.add_node(node_id, None)
        temp_node.sdo.RESPONSE_TIMEOUT = self.app.sdo_timeout
        
        try:
            vendor_id = await temp_node.sdo[0x1018][1].aget_raw()
            product_code = await temp_node.sdo[0x1018][2].aget_raw()
        except (SdoCommunicationError, SdoAbortedError) as e:
            logger.error("Failed to query identity of new node %02x: %s", node_id, e)
            self.unsupported_nodes.add(node_id)
            del self.can_network[node_id]
            return

        device_info = next((d for d in self.app.devices_config if d.get('vendor_id') == vendor_id and d.get('product_code') == product_code), None)

        if not device_info or 'eds_file' not in device_info:
            logger.warning("No matching device config for node %02x (Vendor: %s, Product: %s). Creating informational entity.", node_id, hex(vendor_id), hex(product_code))
            unsupported_entity = EntityRegistry.create(253, temp_node, 0, self.app.mqtt_topic_prefix, 0)
            unsupported_entity.vendor_id = vendor_id
            unsupported_entity.product_code = product_code
            await unsupported_entity.publish_config(self.app.mqtt_manager)
            await unsupported_entity.mqtt_initial_publish(self.app.mqtt_manager)
            self.unsupported_nodes.add(node_id)
            del self.can_network[node_id]
            return

        del self.can_network[node_id]
        od_path = os.path.join(BASE_DIR, "eds", device_info['eds_file'])
        logger.info("Loading EDS '%s' for node %02x", device_info['eds_file'], node_id)
        
        try:
            od = import_od(od_path)
        except FileNotFoundError:
            logger.error(f"EDS file not found for node {node_id} at path {od_path}.")
            self.unsupported_nodes.add(node_id)
            return

        node = self.can_network.add_node(node_id, od)
        node.sdo.RESPONSE_TIMEOUT = self.app.sdo_timeout
        node.device_info = device_info
        
        self._initialize_node_properties(node)
        await self.register_node(node)
        
    def _initialize_node_properties(self, node):
        """Sets default properties for a newly created node object."""
        node_id = node.id
        node.is_initialized = False
        node.is_supported = True
        node.last_heartbeat_time = time.time()
        node.availability = None
        node.availability_topic = f"{self.app.mqtt_topic_prefix}/can_{node_id:03x}/availability"
        node.prod_heartbeat_time = None
        node.device_name = f"CANopen Node {node_id}"
        node.sw_version = "N/A"
        node.hw_version = ""

    async def handle_unconfigured_node_zero(self):
        """Handles the special case of a device at Node ID 0."""
        logger.info("Unconfigured device (node ID 0) detected.")
        temp_node_0 = self.can_network.add_node(UNCONFIGURED_NODE_ID, None)
        temp_node_0.sdo.RESPONSE_TIMEOUT = self.app.sdo_timeout
        try:
            all_node_ids = set(self.can_network.scanner.nodes) | {node.id for node in self.can_network.values()}
            free_node_id = next(i for i in range(1, 128) if i not in all_node_ids)
            
            logger.info("Assigning new node ID %d to unconfigured device.", free_node_id)
            await temp_node_0.sdo[0x2002].aset_raw(free_node_id)
            await temp_node_0.nmt.state_set("RESET NODE")
            logger.info("Device reset. Waiting for it to reappear with new ID %d.", free_node_id)
        except StopIteration:
            logger.error("No free node ID found (1-127).")
        except Exception as e:
            logger.error("Failed to configure new device from ID 0: %s", e)
        finally:
            if UNCONFIGURED_NODE_ID in self.can_network:
                del self.can_network[UNCONFIGURED_NODE_ID]

    async def register_node(self, node):
        """Continues the registration process for a supported node."""
        logger.info("Registering supported node %02x: %s", node.id, node.device_info.get("name", "Unnamed Device"))
        
        try:
            hb_time = await node.sdo["ProducerHeartbeatTime"].aget_raw()
            node.prod_heartbeat_time = hb_time
            node.nmt.add_heartbeat_callback(self._get_heartbeat_cb(node))
        except (SdoAbortedError, SdoCommunicationError):
            logger.warning("Node %02x: Could not read ProducerHeartbeatTime. Availability monitoring will be impaired.", node.id)

        try:
            device_name_from_node = (await node.sdo[0x1008].aget_raw()).strip()
            if not device_name_from_node:
                raise SdoAbortedError("Device name is empty")
            node.device_name = device_name_from_node
            logger.info("Node %02x is already configured as '%s'. Processing entities.", node.id, node.device_name)
            await self.app.device_manager.process_node_entities(node)
        except (SdoAbortedError, SdoCommunicationError):
            logger.info("Node %02x is unconfigured. Publishing configuration entity.", node.id)
            config_entity = EntityRegistry.create(254, node, 254, self.app.mqtt_topic_prefix)
            config_entity.props['name'] = f"Unconfigured Device (Node {node.id})"
            await config_entity.publish_config(self.app.mqtt_manager)
        
        node.is_initialized = True
        await self.update_node_availability(node, "online")

    def _get_heartbeat_cb(self, node):
        """Creates a callback for CANopen heartbeat messages (operational)."""
        async def on_heartbeat(status):
            logger.debug("Heartbeat from %02x: %s", node.id, status)
            node.last_heartbeat_time = time.time()
            await self.update_node_availability(node, "online")

            if status == 0:
                logger.info(f"Node {node.id} has sent a boot-up message. Re-initializing.")
                node.is_initialized = False
                await self.register_node(node)
        return on_heartbeat

    def get_tpdo_cb(self):
        """Creates a callback for CANopen TPDO messages (operational)."""
        async def on_tpdo(map_):
            node_id = map_.pdo_node.node.id
            for var in map_:
                if var.index == 0x2100 and var.subindex == 0:
                    try:
                        state_mask = await var.aget_raw()
                        logger.debug(f"Received TPDO with relay state mask for node {node_id}: {state_mask:08b}")
                        for i in range(8):
                            entity_unique_id = f"can_{node_id:03x}_{(i + 1):02x}"
                            entity = Entity.get_entity_by_unique_id(entity_unique_id)
                            if entity and isinstance(entity, SimpleLight):
                                individual_state = (state_mask >> i) & 1
                                state_key = entity.state_map[0]
                                topic, mqtt_value = entity.get_mqtt_state(state_key, individual_state)
                                await self.app.mqtt_manager.publish(topic, payload=mqtt_value, retain=False)
                    except Exception as e:
                        logger.error(f"Error processing TPDO bitmask for node {node_id}: {e}")
                    return

                try:
                    key = (var.index << 16) | (var.subindex << 8)
                    entity = StateMixin.get_entity_by_node_state_key(node_id, key)
                    if entity:
                        state_topic, value = entity.get_mqtt_state(key, await var.aget_raw())
                        await self.app.mqtt_manager.publish(state_topic, payload=value, retain=False)
                        logger.debug("MQTT TPDO (fallback) publish topic: %s value: %s", state_topic, value)
                except ValueError as e:
                    logger.error("Error publishing fallback TPDO state for node %d: %s", node_id, e)
        return on_tpdo

    async def _check_node_availability(self):
        """Periodically checks if nodes are still available (operational)."""
        while True:
            if self.app.main_watchdog:
                self.app.main_watchdog.reset()

            await asyncio.sleep(10)
            for node in self.can_network.values():
                if node.is_supported and node.prod_heartbeat_time:
                    is_offline = (time.time() - node.last_heartbeat_time) > (2 * node.prod_heartbeat_time / 1000.0)
                    if is_offline and node.availability == "online":
                        await self.update_node_availability(node, "offline")
            
            if self.app.main_watchdog and self.app.main_watchdog.passed():
                raise QuitException("Main watchdog timeout")


    async def update_node_availability(self, node, availability):
        """Publishes a node's availability to MQTT."""
        if node.availability != availability:
            logger.info("Node %02x is now %s", node.id, availability)
            node.availability = availability
            await self.app.mqtt_manager.publish(node.availability_topic, payload=availability, retain=True)
