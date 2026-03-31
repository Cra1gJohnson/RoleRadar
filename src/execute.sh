#!/usr/bin/env bash
set -euo pipefail

LOG_PATH="${HOME}/.cache/google-chrome-apply.log"

nohup google-chrome \
  --remote-debugging-port=9222 \
  --remote-debugging-address=127.0.0.1 \
  --user-data-dir="$HOME/.config/google-chrome-apply" \
  --profile-directory="Profile 1" \
  >"$LOG_PATH" 2>&1 &

echo "Started Chrome in the background with PID $!"
echo "Logs: $LOG_PATH"
