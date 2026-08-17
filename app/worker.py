import asyncio
import logging
from datetime import datetime, timedelta, timezone

from . import db
from .config import settings
from .event_processor import process_event_row
from .pseudogram_client import send_dm, get_dm_status, send_rate_limiter

logger = logging.getLogger("linkplease.worker")


def _backoff_seconds(attempts: int) -> float:
    # 2, 4, 8, 16, 30 (capped)
    return min(2 ** attempts, 30)


async def event_worker() -> None:
    """Continuously drains unprocessed webhook events."""
    while True:
        try:
            rows = await db.fetch_unprocessed_events(limit=25)
            for row in rows:
                try:
                    await process_event_row(row)
                except Exception:
                    logger.exception("Failed processing event_id=%s, will retry next pass", row["event_id"])
            if not rows:
                await asyncio.sleep(settings.EVENT_WORKER_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("event_worker loop error")
            await asyncio.sleep(1)


async def sender_worker() -> None:
    """Sends pending DM jobs to the mock API, respecting the rate limit and
    retrying transient failures with exponential backoff."""
    while True:
        try:
            jobs = await db.fetch_sendable_jobs(limit=10)
            if not jobs:
                await asyncio.sleep(settings.SENDER_WORKER_INTERVAL)
                continue

            for job in jobs:
                await send_rate_limiter.acquire()
                result = await send_dm(
                    recipient_user_id=job["user_id"],
                    message=job["message"],
                    comment_id=job["comment_id"],
                    idempotency_key=job["idempotency_key"],
                )
                attempts = job["attempts"] + 1

                if result.outcome == "accepted":
                    await db.mark_job_queued_api(job["id"], result.dm_id, attempts)

                elif result.outcome == "bad_request":
                    # Retrying won't help -- our payload itself is malformed.
                    await db.mark_job_failed(job["id"], f"bad_request: {result.detail}")

                elif result.outcome == "rate_limited":
                    next_at = datetime.now(timezone.utc) + timedelta(seconds=result.retry_after or 5)
                    await db.mark_job_retry_later(
                        job["id"], attempts, next_at.isoformat(), "rate_limited by mock API"
                    )

                else:  # server_error or network_error -- safe to retry
                    if attempts >= settings.MAX_SEND_ATTEMPTS_PER_CYCLE:
                        await db.mark_job_failed(
                            job["id"], f"{result.outcome} after {attempts} attempts: {result.detail}"
                        )
                    else:
                        delay = _backoff_seconds(attempts)
                        next_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
                        await db.mark_job_retry_later(
                            job["id"], attempts, next_at.isoformat(), f"{result.outcome}: {result.detail}"
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sender_worker loop error")
            await asyncio.sleep(1)


async def reconcile_worker() -> None:
    """Polls the API for the true status of DMs it accepted (202), since
    'accepted' is not 'delivered'. Retries jobs the API reports as failed,
    up to MAX_SEND_CYCLES full fresh-send attempts."""
    while True:
        try:
            jobs = await db.fetch_inflight_jobs(limit=25)
            for job in jobs:
                result = await get_dm_status(job["dm_id"])
                if not result.ok:
                    continue  # transient read error, check again next pass

                if result.status == "delivered":
                    await db.mark_job_delivered(job["id"])
                elif result.status == "failed":
                    cycles = job["send_cycles"] + 1
                    if cycles >= settings.MAX_SEND_CYCLES:
                        await db.mark_job_failed(
                            job["id"], f"API reported failed after {cycles} send cycles"
                        )
                    else:
                        await db.rotate_job_for_new_cycle(
                            job["id"], job["rule_id"], job["user_id"], cycles
                        )
                # status == 'queued' -> still in flight, leave it, check again later

            await asyncio.sleep(settings.RECONCILE_WORKER_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("reconcile_worker loop error")
            await asyncio.sleep(2)


def start_background_workers() -> list[asyncio.Task]:
    return [
        asyncio.create_task(event_worker(), name="event_worker"),
        asyncio.create_task(sender_worker(), name="sender_worker"),
        asyncio.create_task(reconcile_worker(), name="reconcile_worker"),
    ]
