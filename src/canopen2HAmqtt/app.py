import asyncio
import logging

from .can_manager import CanManager
from .mqtt_manager import MqttManager
from .utils import WatchdogTimer, QuitException
from .config import AppConfig
from .device import Device

logger = logging.getLogger(__name__)

class CanOpen2HAmqtt:
    def __init__(self, config: AppConfig):
        self.config = config
        self.main_watchdog = WatchdogTimer(config.watchdog_timeout)
        self.mqtt_manager = MqttManager(self, self.config, self.main_watchdog)
        Device.set_mqtt_manager(self.mqtt_manager)
        self.can_manager = CanManager(self, self.config, self.main_watchdog)
        

async def start(**kwargs):
    app = None
    main_task = None
    
    try:
        config = AppConfig.from_kwargs(**kwargs)
        app = CanOpen2HAmqtt(config)
        
        main_task = asyncio.gather(
            app.can_manager.start(),
            app.mqtt_manager.start(),
        )
        await main_task

    except QuitException as e:
        logger.warning("Application quitting gracefully: %s", e)
        if hasattr(e, 'exit_code'):
            return e.exit_code
        return 1
    except Exception as e:
        logger.exception("Failed to initialize and start the addon: %s", e)
        if main_task:
            main_task.cancel()
        return 1
    finally:
        logger.info("Disconnecting...")
        if app and app.can_manager:
            app.can_manager.shutdown()
        logger.info("Shutdown complete.")
    return 0
