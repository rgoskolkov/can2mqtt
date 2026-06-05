#!/usr/bin/with-contenv bashio
INTERFACE=$(bashio::config 'interface')
CHANNEL=$(bashio::config 'channel')
BITRATE=$(bashio::config 'bitrate')
SERVER=$(bashio::config 'mqtt_server')
TOPIC=$(bashio::config 'mqtt_topic_prefix')
TOPIC=$(test -n "$TOPIC" && echo "-t $TOPIC")
TIMEOUT=$(bashio::config 'sdo_response_timeout' 2.0)
WATCHDOG=$(bashio::config 'watchdog_timeout' 60)
CONFIGURE_CAN=$(bashio::config 'configure_can_interface' true)
EXTRA_ARGS=$(bashio::config 'extra_args')
if test "$EXTRA_ARGS" == null; then
  EXTRA_ARGS=''
fi

ARGS=""
if [[ "$CONFIGURE_CAN" == "true" ]]; then
    ARGS="$ARGS --configure-can-interface"
fi

set -x
canopen2HAmqtt -i "$INTERFACE" -s "$SERVER" -c "$CHANNEL" -b "$BITRATE" $TOPIC --sdo-response-timeout $TIMEOUT --watchdog-timeout $WATCHDOG $ARGS $EXTRA_ARGS
