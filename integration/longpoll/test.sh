#!/bin/bash

# Run confd
./bin/confd --log-level debug --confdir ./integration/confdir --backend longpoll --node http://127.0.0.1:8080 --watch --prefix events
