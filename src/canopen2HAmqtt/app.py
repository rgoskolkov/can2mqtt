import asyncio
import logging

from .can_manager import CanManager
from .device_manager import DeviceManager
from .mqtt_manager import MqttManager
from .utils import WatchdogTimer

logger = logging.getLogger(__name__)

class QuitException(Exception):
    def __init__(self, descr, exit_code=1):
        super().__init__(descr)
        self.exit_code = exit_code

class CanOpen2HAmqtt:
    def __init__(self, **kwargs):
        self.kwargs = kwargs # Store all start args
        self.main_watchdog = None
        
        # Unpack required arguments for easy access
        self.mqtt_server = kwargs.get('mqtt_server')
        self.interface = kwargs.get('interface')
        self.channel = kwargs.get('channel')
        self.bitrate = kwargs.get('bitrate')
        self.mqtt_topic_prefix = kwargs.get('mqtt_topic_prefix')
        self.sdo_timeout = kwargs.get('sdo_response_timeout', 0.5)
        self.devices_config = kwargs.get('devices', [])
        self.configure_can_interface = kwargs.get('configure_can_interface', False)

        # Initialize managers
        self.can_manager = CanManager(self)
        self.device_manager = DeviceManager(self)
        self.mqtt_manager = MqttManager(self)

async def start(**kwargs):
    app = None
    main_task = None
    
    try:
        app = CanOpen2HAmqtt(**kwargs)
        app.main_watchdog = WatchdogTimer(kwargs.get('watchdog_timeout', 60))
        
        main_task = asyncio.gather(
            app.can_manager.start(),
            app.mqtt_manager.start(),
        )
        await main_task

    except QuitException as e:
        logger.warning("Application quitting gracefully: %s", e)
        return e.exit_code
    except Exception as e:
        logger.exception("Failed to initialize and start the addon: %s", e)
        if main_task:
            main_task.cancel()
    finally:
        logger.info("Disconnecting...")
        if app and app.can_manager:
            app.can_manager.shutdown()
        logger.info("Shutdown complete.")
    return 0
