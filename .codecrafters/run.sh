#!/bin/sh
#
# This script is used to run your program on CodeCrafters
#
# This runs after .codecrafters/compile.sh
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit on failure

PORT=6379
[ "$1" = "--port" ] && PORT="$2"

exec /tmp/codecrafters-build-redis-go -port "$PORT"
