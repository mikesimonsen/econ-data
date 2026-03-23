#!/bin/bash
# Wrapper for launchd — appends to single log file, enforces 10 min timeout
# Skips weekends unless the previous run had errors
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
mkdir -p "$LOG_DIR"

DAY_OF_WEEK=$(date +%u)  # 1=Mon ... 6=Sat, 7=Sun

if [ "$DAY_OF_WEEK" -ge 6 ]; then
    # Check last 20 lines of log for error indicators
    LAST_LINES=$(/usr/bin/tail -20 "$LOG_DIR/run.log" 2>/dev/null)
    if echo "$LAST_LINES" | grep -qE "Errors|SKIPPED|KILLED|failed|timed out"; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started (weekend recovery)" >> "$LOG_DIR/run.log"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh skipped (weekend, no errors to recover)" >> "$LOG_DIR/run.log"
        exit 0
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started" >> "$LOG_DIR/run.log"
fi

/opt/homebrew/bin/gtimeout 600 /Users/mikesimonsen/projects/econ-data/.venv/bin/python run.py \
    >> "$LOG_DIR/run.log" 2>&1
EXIT=$?
if [ $EXIT -eq 124 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh KILLED — exceeded 10 min timeout" >> "$LOG_DIR/run.log"
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh finished (exit=$EXIT)" >> "$LOG_DIR/run.log"
