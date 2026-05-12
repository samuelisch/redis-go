#!/bin/sh
#
# This script is used to compile your program on CodeCrafters
#
# This runs before .codecrafters/run.sh
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit on failure

PORT=6379
[ "$1" = "--port" ] && PORT="$2"

go build -o /tmp/codecrafters-build-redis-go app/*.go -port "$PORT"
