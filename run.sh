#!/bin/bash
# Wrapper for launchd — appends to single log file
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
mkdir -p "$LOG_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started" >> "$LOG_DIR/run.log"
PYTHONWARNINGS=ignore /Users/mikesimonsen/projects/econ-data/.venv/bin/python run.py \
    >> "$LOG_DIR/run.log" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh finished (exit=$?)" >> "$LOG_DIR/run.log"
