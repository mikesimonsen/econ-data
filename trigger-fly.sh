#!/bin/bash
# Manually fire the Fly cron pipeline. Returns as soon as the machine starts;
# the actual run happens on Fly's VM, independent of this laptop.
#
# Usage:
#   ./trigger-fly.sh             # morning cohort (default — full fetch)
#   ./trigger-fly.sh intraday    # 4 PM ET cohort (MND + FRED retries)
set -e

COHORT="${1:-morning}"
case "$COHORT" in
  morning)  RUN_SCRIPT=run.py ;;
  intraday) RUN_SCRIPT=run_intraday.py ;;
  *) echo "Unknown cohort: $COHORT (expected morning|intraday)"; exit 1 ;;
esac

APP=econ-data-cron
MACHINE_ID=$(fly machine list -a "$APP" --json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")

echo "Configuring machine $MACHINE_ID (cohort=$COHORT, RUN_SCRIPT=$RUN_SCRIPT)..."
fly machine update "$MACHINE_ID" -a "$APP" \
  --env "RUN_SCRIPT=$RUN_SCRIPT" --skip-start --yes
echo "Starting Fly machine $MACHINE_ID..."
fly machine start "$MACHINE_ID" -a "$APP"
echo "Done. Tail logs with: fly logs -a $APP"
