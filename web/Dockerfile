FROM python:3.12-slim

RUN pip install --no-cache-dir "psycopg[binary]>=3.2"

COPY web/server.py /app/server.py

CMD ["python", "-u", "/app/server.py"]
