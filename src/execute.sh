#!/bin/bash
google-chrome \
  --remote-debugging-port=9222 \
  --user-data-dir=$HOME/.config/google-chrome \
  --profile-directory="Profile 2"