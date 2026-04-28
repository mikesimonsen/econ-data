#!/bin/bash
# Retired. The daily pipeline now runs on Fly, fired by GitHub Actions
# (.github/workflows/cron.yml) at 7 AM ET. The laptop is no longer in the loop.

cat <<'EOF'
run.sh is retired — the pipeline runs on Fly now.

  Trigger manually:   ./trigger-fly.sh
  Tail live logs:     fly logs -a econ-data-cron
  Run locally (debug only): python run.py

The daily 7 AM ET fire is handled by .github/workflows/cron.yml.
EOF

exit 1
