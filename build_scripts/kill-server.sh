#!/bin/bash

PID=$(lsof -ti:8001 -sTCP:LISTEN)

if [ -z "$PID" ]; then
    echo "No process found listening on port 8001"
else
    echo "Killing server 1 (PID: $PID) on port 8001"
    kill -9 $PID
    echo "Done"
fi