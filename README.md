# LinkPlease — Tech Intern Assignment

A small, honest version of "comment PRICE, get a DM" — built on top of the
mock PseudoGram API, which is deliberately unreliable: it duplicates events,
delivers them out of order, rate-limits, 500s, and sometimes accepts a DM it
never actually delivers. This submission is built around that unreliability,
not around the happy path.

**Parts completed: A + B + C**

## Architecture in one paragraph

`POST /webhook` does exactly one thing synchronously: write the raw event to
SQLite with `event_id` as the primary key, then return `200`. That single
write is both the durability guarantee (it's on disk before we ack) and the
dedup mechanism (a redelivered `event_id` fails the `INSERT` and is ignored).
Three background loops do everything else, all driven by polling the
database rather than by in-memory timers or fire-and-forget tasks, which
matters because it means a restart just resumes — nothing is only "remembered"
by a running process:

- **`event_worker`** — drains unprocessed events, matches comment text against
  rules (case-insensitive substring), and creates a `dm_jobs` row per
  matching rule. That row is protected by a `UNIQUE(rule_id, user_id)`
  constraint, which is what actually enforces "never DM the same user twice
  for the same rule" — it holds regardless of how many separate comments or
  redelivered events trigger the match.
- **`sender_worker`** — pulls pending `dm_jobs`, respects the 10-req/60s
  limit with an in-process sliding-window limiter, and calls
  `POST /v1/dm/send` with an `Idempotency-Key`. `429` → backs off using
  `Retry-After`. `500`/network error → exponential backoff, retried under the
  *same* idempotency key (safe — if our first POST actually landed, the API
  just hands back the original `dm_id` instead of sending twice). `400` →
  marked failed immediately, no retry, since the payload itself is bad.
- **`reconcile_worker`** — polls `GET /v1/dm/{dm_id}` for anything sitting at
  `queued_api`, since `202 Accepted` is not `delivered`. If the API later
  reports `failed`, we don't reuse the old idempotency key (the API would
  just hand back the same dead `dm_id`) — we rotate to a fresh key and
  requeue for a genuinely new send attempt, up to `MAX_SEND_CYCLES` times.

`GET /stats` is a live aggregate query over `dm_jobs` (`delivered` → sent,
`failed` → failed, `pending`/`queued_api` → queued) plus a `duplicate_log`
table that records every redelivered `event_id` and every repeat
rule/user match.

`comment.deleted` marks the comment deleted and cancels any `dm_jobs` still
sitting at `pending` for that comment — jobs already sent to the API are left
alone, since there's no way to unsend a DM.

Webhook signature verification (`X-PseudoGram-Signature`) happens before
anything else in the handler, over the raw request bytes, with a
constant-time comparison.

## Running locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in PSEUDOGRAM_API_KEY
export $(cat .env | xargs)
uvicorn app.main:app --reload
```

Then apply for a key and generate it per the assignment README, point
`POST /v1/simulate/start` at your `ngrok`/deployed `/webhook` URL, and check
`GET /v1/simulate/{run_id}/truth` against your own `GET /stats`.

## Deploying

`Dockerfile` + `render.yaml` are set up for Render. The important bit:
**attach the persistent disk** (`render.yaml` mounts one at `/data` and
points `DB_PATH` there) — see `FAILURES.md` for why this matters.

## Tests

```bash
pip install pytest
pytest tests/
```

Covers the pure-logic pieces (signature verification, keyword matching) that
don't need a live DB or network. The real correctness evidence is running
`POST /v1/simulate/start` against a live deployment and diffing `/stats`
against `/v1/simulate/{run_id}/truth` — that's not something a unit test can
substitute for, so I didn't fake one.

## Project layout

```
app/
  main.py              FastAPI app, the three required routes, lifecycle
  db.py                SQLite schema + every query, single source of truth
  worker.py             event_worker / sender_worker / reconcile_worker
  event_processor.py    rule matching, comment.deleted handling
  pseudogram_client.py  HTTP calls to the mock API
  ratelimiter.py        sliding-window limiter for outbound sends
  security.py           HMAC signature verification
  config.py             env-driven settings
  schemas.py             pydantic request/response models
tests/test_logic.py     unit tests for pure functions
FAILURES.md             honest list of how this can still lose/duplicate a DM
```
