import argparse
import asyncio
import logging
import sys
import json
import os

import coloredlogs
import canopen2HAmqtt
import can


def main():
    logging.getLogger("canopen.pdo.base").level = logging.WARNING

    # patch bug in canopen-async
    asyncio.iscouroutine = asyncio.iscoroutine

    parser = argparse.ArgumentParser(
        prog="canopen2HAmqtt",
        description="CAN to MQTT converter",
    )
    parser.add_argument("-s", "--mqtt-server")
    parser.add_argument("-i", "--interface")
    parser.add_argument("-c", "--channel")
    parser.add_argument("-b", "--bitrate")
    parser.add_argument("-j", "--interface-opts-json")
    parser.add_argument("-l", "--log-level", default="INFO")
    parser.add_argument("-t", "--mqtt-topic-prefix")
    parser.add_argument("-d", "--sdo-response-timeout", type=float)
    parser.add_argument("-r", "--sdo-max-retries", type=int)
    parser.add_argument("-w", "--watchdog-timeout", type=int)
    parser.add_argument(
        "--configure-can-interface",
        action="store_true",
        help="Attempt to configure the CAN interface using 'ip link' commands (for SocketCAN on Linux).",
    )
    args = parser.parse_args()

    config_overrides = {
        k: v for k, v in vars(args).items() if v is not None
    }

    config = can.util.load_config(config=config_overrides)

    # Load devices from addon config if running in Home Assistant
    addon_config_path = "/data/options.json"
    if os.path.exists(addon_config_path):
        try:
            with open(addon_config_path, "r") as f:
                addon_config = json.load(f)
            if "devices" in addon_config:
                config["devices"] = addon_config["devices"]
                logging.info("Loaded %d devices from addon config", len(config["devices"]))
        except Exception as e:
            logging.warning("Could not load devices from addon config: %s", e)

    coloredlogs.DEFAULT_LOG_FORMAT = (
        "%(asctime)s %(name)-18s %(levelname)s %(message)s"
    )
    coloredlogs.DEFAULT_LEVEL_STYLES.update(
        {"debug": {"color": 8}, "info": {"color": "green"}}
    )
    coloredlogs.install(level=args.log_level)

    sys.exit(asyncio.run(canopen2HAmqtt.start(**config)))
