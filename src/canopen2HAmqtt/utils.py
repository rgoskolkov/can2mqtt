import logging
import time
from urllib.parse import urlparse
from canopen.sdo.exceptions import SdoAbortedError

# SDO Abort Codes
CODE_SUBINDEX_NOT_FOUND = 0x06090011
CODE_OBJECT_NOT_FOUND = 0x06020000

logger = logging.getLogger(__name__)


class WatchdogTimer:
    def __init__(self, timeout):
        self._timeout = timeout
        self._time = time.time()

    def reset(self):
        self._time = time.time()

    def passed(self):
        return time.time() - self._time >= self._timeout if self._timeout else False


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
