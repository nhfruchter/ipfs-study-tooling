#!/bin/bash
screen -dmS ipfs -X "ipfs daemon"
screen -dmS logging "watch -c -d -n 120 ./makelogs.sh"
screen -dmS dht "./makedht.sh"
