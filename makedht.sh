#!/bin/bash
ipfs log tail | grep -E "handleAdd|handleGet" > dht.$(date +%s).log
