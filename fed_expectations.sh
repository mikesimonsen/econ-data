#!/bin/bash
# Wrapper for launchd — runs weekly fed expectations fetch
# Logs to logs/fed_expectations.log
cd /Users/mikesimonsen/projects/econ-data
LOG_DIR=logs
LOG="$LOG_DIR/fed_expectations.log"
ERR="$LOG_DIR/fed_expectations.error.log"
mkdir -p "$LOG_DIR"

NOW=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$NOW] fed_expectations.sh started" >> "$LOG"

# Refresh the release calendar (discover new dates from agency websites)
echo "[$NOW] Refreshing release calendar..." >> "$LOG"
/opt/homebrew/bin/gtimeout 300 /Users/mikesimonsen/projects/econ-data/.venv/bin/python fetch_expectations.py --refresh-calendar \
    >> "$LOG" 2>&1

# Fetch FedWatch probabilities
/opt/homebrew/bin/gtimeout 600 /Users/mikesimonsen/projects/econ-data/.venv/bin/python fetch_fed_expectations.py --limit 6 \
    >> "$LOG" 2>&1
EXIT=$?

NOW=$(date '+%Y-%m-%d %H:%M:%S')

if [ $EXIT -eq 124 ]; then
    echo "[$NOW] fed_expectations.sh KILLED — exceeded 10 min timeout" >> "$LOG"
    echo "[$NOW] TIMEOUT: fetch_fed_expectations.py exceeded 10 min limit" >> "$ERR"
elif [ $EXIT -ne 0 ]; then
    echo "[$NOW] fed_expectations.sh finished with error (exit=$EXIT)" >> "$LOG"
    echo "[$NOW] CRASH: fetch_fed_expectations.py exited with code $EXIT" >> "$ERR"
    /usr/bin/tail -10 "$LOG" >> "$ERR"
fi

# After fetching, regenerate the briefing so the new probabilities show up
if [ $EXIT -eq 0 ]; then
    /opt/homebrew/bin/gtimeout 300 /Users/mikesimonsen/projects/econ-data/.venv/bin/python -c "
from econ_data.config import load
from econ_data.briefing import generate_briefing
from econ_data.store_sqlite import get_series_captured_today
from pathlib import Path
cfg = load()
html = generate_briefing(cfg, updated_ids=get_series_captured_today())
Path('docs/index.html').write_text(html)
print('Briefing regenerated')
" >> "$LOG" 2>&1

    # Commit and push
    /usr/bin/git -C /Users/mikesimonsen/projects/econ-data add docs/index.html >> "$LOG" 2>&1
    /usr/bin/git -C /Users/mikesimonsen/projects/econ-data commit -m "data: refresh CME FedWatch probabilities" >> "$LOG" 2>&1
    /usr/bin/git -C /Users/mikesimonsen/projects/econ-data push >> "$LOG" 2>&1
fi

echo "[$NOW] fed_expectations.sh finished (exit=$EXIT)" >> "$LOG"
