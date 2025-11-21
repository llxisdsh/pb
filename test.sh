#!/usr/bin/env bash

while true; do
  echo "$(date +%T) Running test..."
  go test
  if [ $? -ne 0 ]; then
    echo "Error encountered. Stopping the loop."
    exit $?
  fi
done
