import os

class Settings:
    # Secret used to verify inbound webhook HMAC signatures AND to authenticate
    # outbound calls to the mock API. Per the assignment, this is your PseudoGram API key.
    PSEUDOGRAM_API_KEY: str = os.environ.get("PSEUDOGRAM_API_KEY", "")

    PSEUDOGRAM_BASE_URL: str = os.environ.get(
        "PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com"
    )

    # Where the SQLite file lives. On Render, mount a persistent disk at /data
    # and point this there, or the DB is wiped on every redeploy/restart.
    DB_PATH: str = os.environ.get("DB_PATH", "./data/linkplease.db")

    # Outbound rate limit imposed by the mock API.
    RATE_LIMIT_MAX_REQUESTS: int = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "10"))
    RATE_LIMIT_WINDOW_SECONDS: int = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

    # How many raw POST attempts we make within one "send cycle" (covers 500s /
    # network errors / timeouts) before giving up on that cycle.
    MAX_SEND_ATTEMPTS_PER_CYCLE: int = int(os.environ.get("MAX_SEND_ATTEMPTS_PER_CYCLE", "5"))

    # How many full send cycles (fresh idempotency key, after the API told us
    # a previous dm_id terminally "failed") we're willing to try before we
    # give up on the DM entirely.
    MAX_SEND_CYCLES: int = int(os.environ.get("MAX_SEND_CYCLES", "3"))

    # Loop intervals (seconds)
    EVENT_WORKER_INTERVAL: float = float(os.environ.get("EVENT_WORKER_INTERVAL", "0.5"))
    SENDER_WORKER_INTERVAL: float = float(os.environ.get("SENDER_WORKER_INTERVAL", "0.5"))
    RECONCILE_WORKER_INTERVAL: float = float(os.environ.get("RECONCILE_WORKER_INTERVAL", "3"))

    HTTP_TIMEOUT_SECONDS: float = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))


settings = Settings()
