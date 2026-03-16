#!/bin/bash
# Wrapper for launchd — appends to log files instead of truncating
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
mkdir -p "$LOG_DIR"
/Users/mikesimonsen/projects/econ-data/.venv/bin/python run.py \
    >> "$LOG_DIR/run.log" 2>> "$LOG_DIR/run.error.log"
