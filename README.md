# CAN to MQTT bridge for Home Assistant
This project provides tools for easy exposing CANopen entities to Home Assistant over CAN Bus:
* It provides `can2mqtt` bridge exposing CANopen entities onto MQTT topics. It follows MQTT Discovery protocol, so entities appear automatically in HomeAssistant.
* It supports firmware upgrades via CANBUS.

## What you need
* A CAN bus adapter supported by [python-can](https://python-can.readthedocs.io/en/master/interfaces.html).
* A CANopen compatible device.

## How to use
The `can2mqtt` bridge is available as a Home Assistant addon. Please refer to the addon documentation for installation and configuration instructions.
