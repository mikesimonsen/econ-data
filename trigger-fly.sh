#!/bin/bash
# Manually fire the Fly cron pipeline. Returns as soon as the machine starts;
# the actual run happens on Fly's VM, independent of this laptop.
set -e

APP=econ-data-cron
MACHINE_ID=$(fly machine list -a "$APP" --json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")

echo "Starting Fly machine $MACHINE_ID in app $APP..."
fly machine start "$MACHINE_ID" -a "$APP"
echo "Done. Tail logs with: fly logs -a $APP"
