import asyncio
import logging
import time
import typing
import struct
import can

from typing import Dict, Optional

from .utils import QuitException, WatchdogTimer
from .config import AppConfig
from .device import Device, UnsupportedDevice
from .device_registry import DEVICE_REGISTRY

if typing.TYPE_CHECKING:
    from .app import CanOpen2HAmqtt

logger = logging.getLogger(__name__)

# CANopen Function Codes
COB_NMT = 0x000
COB_HEARTBEAT = 0x700
COB_SDO_TX = 0x580
COB_SDO_RX = 0x600

SDO_UPLOAD_REQUEST = 0x40
SDO_UPLOAD_RESPONSE = 0x40
SDO_DOWNLOAD_REQUEST = 0x20
SDO_DOWNLOAD_RESPONSE = 0x60
SDO_ABORT = 0x80

SDO_IDX_IDENTITY_OBJECT = 0x1018
SDO_SUBIDX_PRODUCT_CODE = 0x02
SDO_IDX_NODE_ID_CONFIG = 0x2002


class CanManager:
    app: 'CanOpen2HAmqtt'
    devices: Dict[int, Device] = {}
    
    def __init__(self, app: 'CanOpen2HAmqtt', config: AppConfig, watchdog: WatchdogTimer):
        self.app = app
        self.config = config
        self.watchdog = watchdog
        self.bus: Optional[can.BusABC] = None
        self.notifier: Optional[can.Notifier] = None
        self.sdo_response_futures: Dict[int, asyncio.Future] = {}
        self.pdo_handlers: Dict[int, Device] = {}

    def get_nodes(self) -> typing.ValuesView[Device]:
        return self.devices.values()

    def shutdown(self):
        if self.notifier:
            self.notifier.stop()
        if self.bus:
            self.bus.shutdown()

    async def start(self):
        if self.config.interface == 'socketcan' and self.config.configure_can_interface:
            await self._ensure_can_interface_up(self.config.channel, str(self.config.bitrate))

        self.bus = can.Bus(
            interface=self.config.interface,
            channel=self.config.channel,
            bitrate=self.config.bitrate,
            **(self.config.extra_args or {})
        )
        logger.info(f"Connected to CAN bus via {self.config.interface}:{self.config.channel} at {self.config.bitrate} bps")

        self.notifier = can.Notifier(self.bus, [self._on_message], 1, asyncio.get_running_loop())
        asyncio.create_task(self._check_node_availability())

    def _on_message(self, msg: can.Message):
        cob_id = msg.arbitration_id
        
        # SDO Response
        if COB_SDO_TX <= cob_id < COB_SDO_TX + 128:
            if cob_id in self.sdo_response_futures:
                future = self.sdo_response_futures.pop(cob_id)
                if not future.done():
                    future.set_result(msg)
            return

        # Heartbeat / NMT Boot-up
        if COB_HEARTBEAT <= cob_id < COB_HEARTBEAT + 128:
            node_id = cob_id & 0x7F
            if node_id in self.devices:
                self.devices[node_id].last_heartbeat_time = time.time()
                if msg.dlc > 0 and msg.data[0] == 0: # Boot-up message
                    logger.info("Node %d has sent a boot-up message. Re-initializing.", node_id)
                    asyncio.create_task(self.on_node_detected(node_id, force_reinit=True))
            else:
                logger.info("New node %d detected via NMT message.", node_id)
                asyncio.create_task(self.on_node_detected(node_id))
            return
            
        # PDO
        if cob_id in self.pdo_handlers:
            device = self.pdo_handlers[cob_id]
            asyncio.create_task(device.handle_pdo(cob_id, msg.data))

    async def _read_sdo(self, node_id: int, index: int, subindex: int, timeout: float) -> Optional[bytes]:
        cob_rx = COB_SDO_RX + node_id
        cob_tx = COB_SDO_TX + node_id
        request_frame = struct.pack("<BHBxxxx", SDO_UPLOAD_REQUEST, index, subindex)
        future = asyncio.get_running_loop().create_future()
        self.sdo_response_futures[cob_tx] = future
        try:
            await self.send_can_message(cob_rx, request_frame)
            response_msg = await asyncio.wait_for(future, timeout)
            response_cmd = response_msg.data[0]
            if response_cmd == SDO_ABORT:
                abort_code = struct.unpack('<I', response_msg.data[4:8])[0]
                logger.error("SDO Abort from node %d for 0x%04X:%d, code 0x%08X", node_id, index, subindex, abort_code)
                return None
            if (response_cmd & 0xE0) == SDO_UPLOAD_RESPONSE:
                # The 'n' bits (2 and 3) indicate the number of bytes that do NOT contain data
                size_indicator = (response_cmd >> 2) & 0b11
                data_len = 4 - size_indicator
                return response_msg.data[4:4+data_len]
            logger.warning("Unsupported SDO response from node %d: %s", node_id, response_msg.data.hex())
            return None
        except asyncio.TimeoutError:
            logger.error("SDO read timed out for node %d, index 0x%04X", node_id, index)
            return None
        finally:
            if cob_tx in self.sdo_response_futures:
                self.sdo_response_futures.pop(cob_tx, None)
    
    async def _write_node_id(self, node_id: int, data: bytes) -> bool:
        return await self._write_sdo(node_id, SDO_IDX_NODE_ID_CONFIG, 0, data)


    async def _write_sdo(self, node_id: int, index: int, subindex: int, data: bytes) -> bool:
        cob_rx = COB_SDO_RX + node_id
        cob_tx = COB_SDO_TX + node_id
        data_len = len(data)
        command_byte = (SDO_DOWNLOAD_REQUEST & 0xF0) | ((4 - data_len) << 2) | 0b11
        request_frame = struct.pack("<BHB", command_byte, index, subindex) + data.ljust(4, b'\0')
        future = asyncio.get_running_loop().create_future()
        self.sdo_response_futures[cob_tx] = future
        try:
            await self.send_can_message(cob_rx, request_frame)
            response_msg = await asyncio.wait_for(future, self.config.sdo_response_timeout)
            if response_msg.data[0] == SDO_DOWNLOAD_RESPONSE:
                return True
            if response_msg.data[0] == SDO_ABORT:
                abort_code = struct.unpack('<I', response_msg.data[4:8])[0]
                logger.error("SDO Abort on write to node %d for 0x%04X:%d, code 0x%08X", node_id, index, subindex, abort_code)
            return False
        except asyncio.TimeoutError:
            logger.error("SDO write timed out for node %d, index 0x%04X", node_id, index)
            return False
        finally:
            if cob_tx in self.sdo_response_futures:
                self.sdo_response_futures.pop(cob_tx, None)

    async def on_node_detected(self, node_id: int, force_reinit: bool = False):
        if node_id in self.devices and self.devices[node_id].is_initialized and not force_reinit:
            return

        logger.info("Identifying new node %d...", node_id)
        product_code_bytes = await self._read_sdo(node_id, SDO_IDX_IDENTITY_OBJECT, SDO_SUBIDX_PRODUCT_CODE, self.config.sdo_response_timeout)
        
        if not product_code_bytes:
            logger.error("Failed to read Product Code from node %d.", node_id)
            return

        product_code = struct.unpack('<I', product_code_bytes)[0]
        device_class = DEVICE_REGISTRY.get(product_code)
        
        if device_class:
            logger.info("Node %d identified as '%s'", node_id, device_class.MODEL_NAME)
            device = device_class(self.config.mqtt_topic_prefix, node_id)
            self.devices[node_id] = device
            await device.initialize(self)
            device.is_initialized = True
        else:
            logger.warning("Node %d (PID: 0x%X) is not in the device registry. Creating UnsupportedDevice.", node_id, product_code)
            device = UnsupportedDevice(self.config.mqtt_topic_prefix, node_id, product_code)
            self.devices[node_id] = device
            await device.initialize(self)
            device.is_initialized = True

    async def _check_node_availability(self):
        while True:
            await asyncio.sleep(10)
            if self.watchdog:
                self.watchdog.reset()
            for node_id, device in list(self.devices.items()):
                hb_timeout = (device.prod_heartbeat_time * 2 / 1000.0) if device.prod_heartbeat_time else 20.0
                if (time.time() - device.last_heartbeat_time) > hb_timeout and device.availability == "online":
                    await device.publish_availability("offline")
                elif (time.time() - device.last_heartbeat_time) <= hb_timeout and device.availability == "offline":
                    await device.publish_availability("online")
            if self.watchdog and self.watchdog.passed():
                raise QuitException("Main watchdog timeout")

    def register_pdo_handler(self, cob_id: int, device: Device):
        self.pdo_handlers[cob_id] = device

    async def send_can_message(self, cob_id: int, data: bytes):
        if not self.bus:
            logger.error("CAN bus is not available.")
            return
        message = can.Message(arbitration_id=cob_id, data=data, is_extended_id=False)
        try:
            self.bus.send(message)
        except can.CanError as e:
            logger.error("Error sending CAN message: %s", e)

    async def _ensure_can_interface_up(self, device: str, bitrate: str):
        """Uses the '/sbin/ip' command to configure and bring up the CAN interface."""
        logging.info(f"Attempting to configure CAN interface '{device}' using /sbin/ip...")
        
        pre_config_cmd = f"/sbin/ip -details link show {device}"
        pre_config_status_proc = await asyncio.create_subprocess_shell(
            pre_config_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        pre_config_stdout, pre_config_stderr = await pre_config_status_proc.communicate()
        logging.info(f"Command: '{pre_config_cmd}' RC: {pre_config_status_proc.returncode} STDOUT: {pre_config_stdout.decode().strip()} STDERR: {pre_config_stderr.decode().strip()}")

        try:
            down_cmd = f"/sbin/ip link set {device} down || true"
            down_proc = await asyncio.create_subprocess_shell(
                down_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await down_proc.communicate()
            
            set_bitrate_cmd = f"/sbin/ip link set {device} type can bitrate {bitrate}"
            proc_set = await asyncio.create_subprocess_shell(set_bitrate_cmd, stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            _, stderr_bytes = await proc_set.communicate()
            stderr_str = stderr_bytes.decode().strip()
            if proc_set.returncode != 0 and "File exists" not in stderr_str:
                raise RuntimeError(f"Failed to set CAN type and bitrate for '{device}': {stderr_str}")
        
            up_cmd = f"/sbin/ip link set {device} up"
            await asyncio.create_subprocess_shell(up_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            
            logging.info(f"CAN interface '{device}' configured successfully using /sbin/ip.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during CAN interface setup for '{device}': {e}")
            raise
