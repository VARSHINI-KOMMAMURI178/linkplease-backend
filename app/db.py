import os
import json
import uuid
import asyncio
from datetime import datetime, timezone
from typing import Optional, Any

import aiosqlite

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id     TEXT PRIMARY KEY,
    keyword     TEXT NOT NULL,
    dm_message  TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    comment_id   TEXT,
    raw_payload  TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    processed    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_processed ON events(processed);

CREATE TABLE IF NOT EXISTS comments (
    comment_id  TEXT PRIMARY KEY,
    post_id     TEXT,
    text        TEXT,
    user_id     TEXT,
    username    TEXT,
    created_at  TEXT,
    deleted     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dm_jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id          TEXT NOT NULL,
    comment_id       TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    message          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    -- pending | queued_api | delivered | failed | cancelled
    dm_id            TEXT,
    idempotency_key  TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    send_cycles      INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    next_attempt_at  TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE(rule_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_dmjobs_status ON dm_jobs(status);
CREATE INDEX IF NOT EXISTS idx_dmjobs_comment ON dm_jobs(comment_id);

CREATE TABLE IF NOT EXISTS duplicate_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,   -- 'event_redelivered' | 'rule_user_repeat'
    ref         TEXT,
    created_at  TEXT NOT NULL
);
"""

_conn: Optional[aiosqlite.Connection] = None
_write_lock = asyncio.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def init_db() -> None:
    global _conn
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    _conn = await aiosqlite.connect(settings.DB_PATH)
    _conn.row_factory = aiosqlite.Row
    # WAL mode: readers don't block the writer, and vice versa. We still
    # serialize writes ourselves with _write_lock since SQLite allows only
    # one writer at a time regardless.
    await _conn.execute("PRAGMA journal_mode=WAL;")
    await _conn.execute("PRAGMA foreign_keys=ON;")
    await _conn.executescript(SCHEMA)
    await _conn.commit()


async def close_db() -> None:
    if _conn:
        await _conn.close()


def get_conn() -> aiosqlite.Connection:
    assert _conn is not None, "DB not initialized"
    return _conn


# ---------- rules ----------

async def create_rule(keyword: str, dm_message: str) -> dict:
    rule_id = f"rule_{uuid.uuid4().hex[:12]}"
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
            (rule_id, keyword, dm_message, now_iso()),
        )
        await _conn.commit()
    return {"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}


async def get_all_rules() -> list[aiosqlite.Row]:
    cur = await _conn.execute("SELECT * FROM rules")
    return await cur.fetchall()


# ---------- events ----------

async def insert_event_if_new(event_id: str, event_type: str, comment_id: Optional[str], raw_payload: dict) -> bool:
    """Returns True if this event_id was new (inserted), False if it's a redelivery."""
    async with _write_lock:
        try:
            await _conn.execute(
                "INSERT INTO events (event_id, event_type, comment_id, raw_payload, received_at, processed) "
                "VALUES (?, ?, ?, ?, ?, 0)",
                (event_id, event_type, comment_id, json.dumps(raw_payload), now_iso()),
            )
            await _conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def log_duplicate(kind: str, ref: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO duplicate_log (kind, ref, created_at) VALUES (?, ?, ?)",
            (kind, ref, now_iso()),
        )
        await _conn.commit()


async def fetch_unprocessed_events(limit: int = 25) -> list[aiosqlite.Row]:
    cur = await _conn.execute(
        "SELECT * FROM events WHERE processed = 0 ORDER BY received_at ASC LIMIT ?", (limit,)
    )
    return await cur.fetchall()


async def mark_event_processed(event_id: str) -> None:
    async with _write_lock:
        await _conn.execute("UPDATE events SET processed = 1 WHERE event_id = ?", (event_id,))
        await _conn.commit()


# ---------- comments ----------

async def upsert_comment(comment_id: str, post_id, text, user_id, username, created_at) -> None:
    async with _write_lock:
        await _conn.execute(
            """
            INSERT INTO comments (comment_id, post_id, text, user_id, username, created_at, deleted)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(comment_id) DO UPDATE SET
                post_id=excluded.post_id, text=excluded.text,
                user_id=excluded.user_id, username=excluded.username,
                created_at=excluded.created_at
            """,
            (comment_id, post_id, text, user_id, username, created_at),
        )
        await _conn.commit()


async def get_comment(comment_id: str) -> Optional[aiosqlite.Row]:
    cur = await _conn.execute("SELECT * FROM comments WHERE comment_id = ?", (comment_id,))
    return await cur.fetchone()


async def mark_comment_deleted(comment_id: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO comments (comment_id, deleted) VALUES (?, 1) "
            "ON CONFLICT(comment_id) DO UPDATE SET deleted = 1",
            (comment_id,),
        )
        await _conn.commit()


# ---------- dm_jobs ----------

async def create_dm_job_if_new(rule_id: str, comment_id: str, user_id: str, message: str) -> bool:
    """Returns True if a new dm_job was created, False if (rule_id, user_id) already exists
    (i.e. this user already got / is getting a DM for this rule -> duplicate, don't send again)."""
    idem_key = f"{rule_id}:{user_id}:0"
    async with _write_lock:
        try:
            await _conn.execute(
                """
                INSERT INTO dm_jobs
                    (rule_id, comment_id, user_id, message, status, idempotency_key,
                     attempts, send_cycles, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, 0, 0, ?, ?)
                """,
                (rule_id, comment_id, user_id, message, idem_key, now_iso(), now_iso()),
            )
            await _conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def cancel_pending_jobs_for_comment(comment_id: str) -> int:
    """Cancel any DM for this comment that hasn't been sent to the API yet.
    Returns number of rows cancelled."""
    async with _write_lock:
        cur = await _conn.execute(
            "UPDATE dm_jobs SET status = 'cancelled', updated_at = ? "
            "WHERE comment_id = ? AND status = 'pending'",
            (now_iso(), comment_id),
        )
        await _conn.commit()
        return cur.rowcount


async def fetch_sendable_jobs(limit: int = 10) -> list[aiosqlite.Row]:
    cur = await _conn.execute(
        """
        SELECT * FROM dm_jobs
        WHERE status = 'pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (now_iso(), limit),
    )
    return await cur.fetchall()


async def fetch_inflight_jobs(limit: int = 25) -> list[aiosqlite.Row]:
    cur = await _conn.execute(
        "SELECT * FROM dm_jobs WHERE status = 'queued_api' AND dm_id IS NOT NULL LIMIT ?",
        (limit,),
    )
    return await cur.fetchall()


async def mark_job_queued_api(job_id: int, dm_id: str, attempts: int) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE dm_jobs SET status='queued_api', dm_id=?, attempts=?, "
            "last_error=NULL, next_attempt_at=NULL, updated_at=? WHERE id=?",
            (dm_id, attempts, now_iso(), job_id),
        )
        await _conn.commit()


async def mark_job_retry_later(job_id: int, attempts: int, next_attempt_at: str, error: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE dm_jobs SET attempts=?, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
            (attempts, next_attempt_at, error, now_iso(), job_id),
        )
        await _conn.commit()


async def mark_job_failed(job_id: int, error: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE dm_jobs SET status='failed', last_error=?, updated_at=? WHERE id=?",
            (error, now_iso(), job_id),
        )
        await _conn.commit()


async def mark_job_delivered(job_id: int) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE dm_jobs SET status='delivered', updated_at=? WHERE id=?",
            (now_iso(), job_id),
        )
        await _conn.commit()


async def rotate_job_for_new_cycle(job_id: int, rule_id: str, user_id: str, send_cycles: int) -> None:
    """After the API reports a dm_id as terminally 'failed', start a fresh send
    cycle: new idempotency key (so the API doesn't just hand back the same
    failed dm_id), cleared dm_id, back to pending."""
    new_key = f"{rule_id}:{user_id}:{send_cycles}"
    async with _write_lock:
        await _conn.execute(
            """
            UPDATE dm_jobs
            SET status='pending', dm_id=NULL, idempotency_key=?, attempts=0,
                send_cycles=?, next_attempt_at=NULL, updated_at=?
            WHERE id=?
            """,
            (new_key, send_cycles, now_iso(), job_id),
        )
        await _conn.commit()


# ---------- stats ----------

async def get_stats() -> dict:
    cur = await _conn.execute("SELECT status, COUNT(*) c FROM dm_jobs GROUP BY status")
    rows = await cur.fetchall()
    counts = {r["status"]: r["c"] for r in rows}

    cur2 = await _conn.execute("SELECT COUNT(*) c FROM duplicate_log")
    dup_row = await cur2.fetchone()

    sent = counts.get("delivered", 0)
    failed = counts.get("failed", 0)
    queued = counts.get("pending", 0) + counts.get("queued_api", 0)
    duplicates_blocked = dup_row["c"]

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }
