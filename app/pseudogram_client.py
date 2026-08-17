import httpx
from dataclasses import dataclass
from typing import Optional

from .config import settings
from .ratelimiter import SlidingWindowRateLimiter

send_rate_limiter = SlidingWindowRateLimiter(
    settings.RATE_LIMIT_MAX_REQUESTS, settings.RATE_LIMIT_WINDOW_SECONDS
)


@dataclass
class SendResult:
    outcome: str  # 'accepted' | 'rate_limited' | 'server_error' | 'bad_request' | 'network_error'
    dm_id: Optional[str] = None
    retry_after: Optional[float] = None
    detail: Optional[str] = None


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=settings.PSEUDOGRAM_BASE_URL,
        headers={"X-API-Key": settings.PSEUDOGRAM_API_KEY},
        timeout=settings.HTTP_TIMEOUT_SECONDS,
    )


async def send_dm(recipient_user_id: str, message: str, comment_id: str, idempotency_key: str) -> SendResult:
    """
    Note: this call itself counts against the 10/60s rate limit, so the
    caller is expected to have already gone through send_rate_limiter.acquire()
    before invoking this.
    """
    body = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id,
    }
    headers = {"Idempotency-Key": idempotency_key}
    try:
        async with _client() as client:
            resp = await client.post("/v1/dm/send", json=body, headers=headers)
    except (httpx.TimeoutException, httpx.TransportError) as e:
        return SendResult(outcome="network_error", detail=str(e))

    if resp.status_code == 202:
        data = resp.json()
        return SendResult(outcome="accepted", dm_id=data.get("dm_id"))
    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        try:
            retry_after_f = float(retry_after) if retry_after else 5.0
        except ValueError:
            retry_after_f = 5.0
        return SendResult(outcome="rate_limited", retry_after=retry_after_f)
    if resp.status_code == 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        return SendResult(outcome="bad_request", detail=detail)
    # 500 and anything else unexpected: treat as a transient server error.
    return SendResult(outcome="server_error", detail=f"HTTP {resp.status_code}: {resp.text[:200]}")


@dataclass
class StatusResult:
    ok: bool
    status: Optional[str] = None  # 'queued' | 'delivered' | 'failed'
    detail: Optional[str] = None


async def get_dm_status(dm_id: str) -> StatusResult:
    """Reads don't count against the rate limit, so no limiter here."""
    try:
        async with _client() as client:
            resp = await client.get(f"/v1/dm/{dm_id}")
    except (httpx.TimeoutException, httpx.TransportError) as e:
        return StatusResult(ok=False, detail=str(e))

    if resp.status_code == 200:
        return StatusResult(ok=True, status=resp.json().get("status"))
    return StatusResult(ok=False, detail=f"HTTP {resp.status_code}: {resp.text[:200]}")
