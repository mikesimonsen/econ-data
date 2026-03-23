#!/bin/bash
# Wrapper for launchd — appends to single log file, enforces 10 min timeout
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
mkdir -p "$LOG_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started" >> "$LOG_DIR/run.log"
/opt/homebrew/bin/gtimeout 600 /Users/mikesimonsen/projects/econ-data/.venv/bin/python run.py \
    >> "$LOG_DIR/run.log" 2>&1
EXIT=$?
if [ $EXIT -eq 124 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh KILLED — exceeded 10 min timeout" >> "$LOG_DIR/run.log"
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh finished (exit=$EXIT)" >> "$LOG_DIR/run.log"
