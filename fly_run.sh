#!/bin/bash
# Cron entrypoint. Clones latest main fresh, runs the pipeline, exits.
# The pipeline itself handles the git commit/push of regenerated outputs.
#
# RUN_SCRIPT picks the entrypoint:
#   run.py           — morning cohort (default)
#   run_intraday.py  — 4 PM ET cohort (MND + FRED retries)
set -e

WORK=/tmp/econ-data
rm -rf "$WORK"

REPO="${GITHUB_REPO:-mikesimonsen/econ-data}"
git clone --depth 50 "https://${GITHUB_TOKEN}@github.com/${REPO}.git" "$WORK"
cd "$WORK"

# Committer identity for the pipeline's `git commit` step.
git config user.email "${GIT_AUTHOR_EMAIL:-cron@cognitaresearch.com}"
git config user.name "${GIT_AUTHOR_NAME:-econ-data cron}"

python "${RUN_SCRIPT:-run.py}"
