#!/bin/bash
# Wrapper for launchd — appends to single log file, enforces 20 min timeout
# Skips weekends unless the previous run had errors
# Writes to run.error.log when anything goes wrong
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
LOG="$LOG_DIR/run.log"
ERR="$LOG_DIR/run.error.log"
mkdir -p "$LOG_DIR"

DAY_OF_WEEK=$(date +%u)  # 1=Mon ... 6=Sat, 7=Sun

if [ "$DAY_OF_WEEK" -ge 6 ]; then
    # Check last 20 lines of log for error indicators
    LAST_LINES=$(/usr/bin/tail -20 "$LOG" 2>/dev/null)
    if echo "$LAST_LINES" | grep -qE "Errors|SKIPPED|KILLED|failed|timed out"; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started (weekend recovery)" >> "$LOG"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh skipped (weekend, no errors to recover)" >> "$LOG"
        exit 0
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] run.sh started" >> "$LOG"
fi

/opt/homebrew/bin/gtimeout 1200 /Users/mikesimonsen/projects/econ-data/.venv/bin/python run.py \
    >> "$LOG" 2>&1
EXIT=$?

NOW=$(date '+%Y-%m-%d %H:%M:%S')

if [ $EXIT -eq 124 ]; then
    echo "[$NOW] run.sh KILLED — exceeded 20 min timeout" >> "$LOG"
    echo "[$NOW] TIMEOUT: run.py exceeded 20 min limit and was killed" >> "$ERR"
elif [ $EXIT -ne 0 ]; then
    echo "[$NOW] run.sh finished with error (exit=$EXIT)" >> "$LOG"
    echo "[$NOW] CRASH: run.py exited with code $EXIT" >> "$ERR"
    echo "  Last 10 lines of run.log:" >> "$ERR"
    /usr/bin/tail -10 "$LOG" >> "$ERR"
    echo "" >> "$ERR"
fi

# Check if the run.log itself reports errors (even with exit 0)
RECENT=$(/usr/bin/tail -20 "$LOG")
if echo "$RECENT" | grep -qE "Errors \(|SKIPPED|failed:"; then
    if ! echo "$RECENT" | grep -q "Done."; then
        echo "[$NOW] INCOMPLETE: run.py finished but with errors" >> "$ERR"
        echo "$RECENT" | grep -E "Errors|SKIPPED|failed" >> "$ERR"
        echo "" >> "$ERR"
    else
        echo "[$NOW] WARNING: run completed but some series had errors" >> "$ERR"
        echo "$RECENT" | grep -E "Errors|SKIPPED|failed" >> "$ERR"
        echo "" >> "$ERR"
    fi
fi

echo "[$NOW] run.sh finished (exit=$EXIT)" >> "$LOG"
