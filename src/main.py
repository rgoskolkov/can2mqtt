import argparse
import asyncio
import logging
import sys

import coloredlogs
import canopen2HAmqtt
from canopen2HAmqtt.config import AppConfig


def main():
    logging.getLogger("can").setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        prog="canopen2HAmqtt",
        description="CAN to MQTT converter",
    )
    parser.add_argument("-s", "--mqtt-server")
    parser.add_argument("-i", "--interface")
    parser.add_argument("-c", "--channel")
    parser.add_argument("-b", "--bitrate")
    parser.add_argument("-j", "--interface-opts-json")
    parser.add_argument("-l", "--log-level", default="DEBUG")
    parser.add_argument("-t", "--mqtt-topic-prefix")
    parser.add_argument("-d", "--sdo-response-timeout", type=float)
    parser.add_argument("-w", "--watchdog-timeout", type=int)
    parser.add_argument(
        "--configure-can-interface",
        action="store_true",
        help="Attempt to configure the CAN interface using 'ip link' commands (for SocketCAN on Linux).",
    )
    args = parser.parse_args()

    # Build config directly from args
    config = AppConfig.from_kwargs(**vars(args))

    coloredlogs.DEFAULT_LOG_FORMAT = (
        "%(asctime)s %(name)-18s %(levelname)s %(message)s"
    )
    coloredlogs.DEFAULT_LEVEL_STYLES.update(
        {"debug": {"color": 8}, "info": {"color": "green"}}
    )
    coloredlogs.install(level=config.log_level)
    sys.exit(asyncio.run(canopen2HAmqtt.start(**vars(config))))


if __name__ == "__main__":
    main()
