import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from . import db
from .config import settings
from .security import verify_signature
from .schemas import CreateRuleRequest, CreateRuleResponse, StatsResponse
from .worker import start_background_workers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("linkplease.main")

_background_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    global _background_tasks
    _background_tasks = start_background_workers()
    logger.info("LinkPlease started. Background workers running.")
    yield
    for t in _background_tasks:
        t.cancel()
    await db.close_db()


app = FastAPI(title="LinkPlease", lifespan=lifespan)


@app.get("/")
async def root():
    return {"service": "linkplease", "status": "ok"}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(request: Request):
    raw_body = await request.body()

    sig_header = request.headers.get("X-PseudoGram-Signature", "")
    if not verify_signature(raw_body, sig_header, settings.PSEUDOGRAM_API_KEY):
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON body")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="missing event_id or event_type")

    comment_id = (payload.get("data") or {}).get("comment_id")

    # This insert is the ONLY thing we do synchronously. It's a fast, atomic
    # write that both acknowledges the event durably (survives a restart)
    # and de-duplicates redeliveries via the event_id primary key. Actual
    # rule-matching / DM-sending happens in the background event_worker,
    # so this handler always returns well under the 5s limit.
    is_new = await db.insert_event_if_new(event_id, event_type, comment_id, payload)
    if not is_new:
        await db.log_duplicate("event_redelivered", event_id)

    return JSONResponse({"status": "accepted"}, status_code=200)


@app.post("/rules", response_model=CreateRuleResponse, status_code=201)
async def create_rule(req: CreateRuleRequest):
    rule = await db.create_rule(req.keyword, req.dm_message)
    return rule


@app.get("/stats", response_model=StatsResponse)
async def stats():
    return await db.get_stats()
