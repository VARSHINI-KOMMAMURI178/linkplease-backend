# FAILURES.md

Honest list of the ways this system can still lose a DM, send a duplicate,
or report a wrong number, and exactly why.

## 1. SQLite on an ephemeral disk is not durable — it just feels durable

Everything I wrote (events table dedup, DB-driven worker loops, idempotency
keys) is designed around "the SQLite file is the source of truth and
survives a restart." That's only true if `DB_PATH` points at a persistent
disk. On a bare Render web service (no disk attached), the filesystem is
wiped on every redeploy and on some restarts. `render.yaml` in this repo
does mount a persistent disk at `/data` — but if that config gets dropped,
or this gets deployed on a plain free-tier instance without a disk, every
guarantee in this README quietly stops being true and nobody would notice
until the numbers looked wrong after a redeploy.

## 2. The rate limiter is per-process, not per-key

`SlidingWindowRateLimiter` tracks the last 10 requests in an in-memory
`deque` inside a single running process. That's correct for exactly one
running instance. If this were ever deployed with more than one instance
behind a load balancer (or two dev environments sharing the same API key
by accident), each process would independently believe it has its own
10-req/60s budget, and the *actual* combined request rate against the
mock API could exceed the real limit even though no single process ever
saw a `429` it didn't expect.

## 3. Cancel-vs-send race on `comment.deleted`

`cancel_pending_jobs_for_comment` only cancels jobs still at `status='pending'`.
If `comment.deleted` is processed by `event_worker` in the same instant that
`sender_worker` has already read that row out of `fetch_sendable_jobs` (but
hasn't written `queued_api` back yet), the cancel `UPDATE` finds nothing to
cancel — the row is mid-flight, not `pending` anymore in spirit, but the
send still goes out. This window is small (both loops run every 0.5s and
the read-then-write isn't a single transaction across the two loops) but
it's real, and I did not attempt to close it with a two-phase claim because
I ran out of time to test it wasn't going to introduce a worse deadlock
under the 500-events-in-10s load test.

## 4. A dm_job the API later reports "delivered" after we've already
   rotated it to a new send cycle will double-send

`reconcile_worker` treats the mock API's `failed` status as terminal and
rotates to a fresh idempotency key + fresh send. If the API's `failed`
status is not actually final — i.e. it flips back to `delivered` sometime
after we've already re-sent under the new key — we'd have two accepted
sends for the same rule/user pointing at two different `dm_id`s, and only
the newest is tracked in `dm_jobs` (the old `dm_id` is discarded, not
polled again). I read the assignment's stated behavior ("roughly 15% of
accepted DMs end up as failed") as `failed` being final, but the README
doesn't explicitly guarantee that, and I didn't hit this in my test runs
because I didn't run enough volume to see it either way.

## 5. `Retry-After` is trusted at face value

On `429`, I schedule the next attempt using whatever `Retry-After` value the
mock API sends, with a fixed 5s fallback if it's missing or unparseable.
I didn't add a ceiling on how long we'll wait even if the API sends back
something absurd (e.g. a very large value) — a misbehaving or adversarial
`Retry-After` could stall a job far longer than intended, and `queued`
in `/stats` would look inflated relative to how "stuck" the job actually is.

## 6. Bulk backfill on startup isn't rate-aware across job types

If the process restarts with a large backlog of both `pending` sends and
`queued_api` jobs awaiting reconciliation, `sender_worker` and
`reconcile_worker` both start hitting the API immediately and independently.
Reconciliation reads don't count against the rate limit per the spec, so
that part is fine, but if I'm wrong about that in practice (e.g. the mock
API actually does count reads under load), the two loops competing for the
same 10-req/60s budget isn't something I load-tested for that specific
scenario.
