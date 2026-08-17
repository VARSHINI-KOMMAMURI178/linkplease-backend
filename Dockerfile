FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Mount a persistent disk at /data in production, or the SQLite file
# (and every DM's history) is wiped on every redeploy/restart.
ENV DB_PATH=/data/linkplease.db
RUN mkdir -p /data

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
