import logging
import time
from urllib.parse import urlparse

# SDO Abort Codes
CODE_SUBINDEX_NOT_FOUND = 0x06090011
CODE_OBJECT_NOT_FOUND = 0x06020000

logger = logging.getLogger(__name__)


class QuitException(Exception):
    def __init__(self, descr, exit_code=1):
        super().__init__(descr)
        self.exit_code = exit_code


class WatchdogTimer:
    def __init__(self, timeout):
        self._timeout = timeout
        self._time = time.time()

    def reset(self):
        self._time = time.time()

    def passed(self):
        return time.time() - self._time >= self._timeout if self._timeout else False


def parse_mqtt_server_url(mqtt_server: str):
    extra_auth = {}
    if mqtt_server.startswith("mqtt://"):
        parsed = urlparse(mqtt_server)
        mqtt_server = parsed.hostname
        extra_auth = dict(
            username=parsed.username,
            password=parsed.password,
            port=int(parsed.port or 1883),
        )
    return mqtt_server, extra_auth
