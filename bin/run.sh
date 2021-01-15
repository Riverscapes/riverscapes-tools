#!/bin/bash
set -eu
IFS=$'\n\t'

echo "$SHELL_SCRIPT" > /usr/local/runner.sh
chmod +x /usr/local/runner.sh

( exec "/usr/local/runner.sh" )