import json
import logging

from . import db

logger = logging.getLogger("linkplease.events")


def keyword_matches(comment_text: str, keyword: str) -> bool:
    """Case-insensitive substring match, anywhere in the comment text."""
    if not comment_text or not keyword:
        return False
    return keyword.lower() in comment_text.lower()


async def process_event_row(event_row) -> None:
    """
    Processes a single row from the `events` table. Idempotent by design:
    - re-running this for an already-fully-processed event is safe because
      dm_job creation is itself guarded by the UNIQUE(rule_id, user_id)
      constraint, so re-matching rules never double-creates a job.
    - marking `processed = 1` happens only after everything below succeeds,
      so a crash mid-way just means this event gets retried on the next
      event_worker pass after restart.
    """
    payload = json.loads(event_row["raw_payload"])
    event_type = event_row["event_type"]
    data = payload.get("data", {})

    if event_type == "comment.created":
        comment_id = data.get("comment_id")
        post_id = data.get("post_id")
        text = data.get("text", "")
        created_at = data.get("created_at")
        from_user = data.get("from", {}) or {}
        user_id = from_user.get("user_id")
        username = from_user.get("username")

        if not comment_id or not user_id:
            logger.warning("comment.created missing comment_id/user_id, skipping: %s", payload)
            await db.mark_event_processed(event_row["event_id"])
            return

        await db.upsert_comment(comment_id, post_id, text, user_id, username, created_at)

        # If this comment was already deleted (comment.deleted arrived first
        # because delivery order isn't guaranteed), don't create any DM jobs.
        existing = await db.get_comment(comment_id)
        if existing and existing["deleted"]:
            await db.mark_event_processed(event_row["event_id"])
            return

        rules = await db.get_all_rules()
        for rule in rules:
            if keyword_matches(text, rule["keyword"]):
                created = await db.create_dm_job_if_new(
                    rule_id=rule["rule_id"],
                    comment_id=comment_id,
                    user_id=user_id,
                    message=rule["dm_message"],
                )
                if not created:
                    # This user already has (or is getting) a DM for this rule.
                    await db.log_duplicate("rule_user_repeat", f"{rule['rule_id']}:{user_id}")

        await db.mark_event_processed(event_row["event_id"])

    elif event_type == "comment.deleted":
        comment_id = data.get("comment_id")
        if not comment_id:
            await db.mark_event_processed(event_row["event_id"])
            return
        await db.mark_comment_deleted(comment_id)
        # Only cancels jobs still sitting at 'pending' (never sent to the API).
        # Jobs already in flight (queued_api) or resolved (delivered/failed)
        # are left alone -- we can't unsend a DM.
        await db.cancel_pending_jobs_for_comment(comment_id)
        await db.mark_event_processed(event_row["event_id"])

    else:
        logger.info("Ignoring unknown event_type=%s", event_type)
        await db.mark_event_processed(event_row["event_id"])
