FROM python:3.12-slim

# git is needed because fly_run.sh clones the repo fresh on every cron firing
# (so we don't have to rebuild the image when code changes — only when deps do)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python deps. Mirrors pyproject.toml. Pinned to lower bounds — psycopg
# binary wheel is the heaviest at ~5 MB.
RUN pip install --no-cache-dir \
    "anthropic>=0.40" \
    "beautifulsoup4>=4.12" \
    "fredapi>=0.5" \
    "openpyxl>=3.1" \
    "psycopg[binary]>=3.2" \
    "python-dotenv>=1.0" \
    "requests>=2.31"

# Entrypoint only — repo contents come from the runtime git clone.
COPY fly_run.sh /usr/local/bin/fly_run.sh
RUN chmod +x /usr/local/bin/fly_run.sh

CMD ["/usr/local/bin/fly_run.sh"]
