from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass
class AppConfig:
    interface: str = 'socketcan'
    channel: str = 'can0'
    bitrate: int = 125000
    mqtt_server: str = 'mqtt://core-mosquitto'
    mqtt_topic_prefix: str = 'homeassistant'
    sdo_response_timeout: float = 2.0
    watchdog_timeout: int = 60
    configure_can_interface: bool = True
    log_level: str = "INFO"
    extra_args: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_kwargs(cls, **kwargs):
        # This factory method helps create the config object from the raw dictionary.
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in known_fields}
        return cls(**filtered_kwargs)
