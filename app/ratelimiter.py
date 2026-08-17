import asyncio
import time
from collections import deque


class SlidingWindowRateLimiter:
    """
    Caps calls to N per rolling window_seconds. In-process only — see
    FAILURES.md for what that means if this ever runs as >1 instance.
    """

    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= self.window_seconds:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return
                # Need to wait until the oldest timestamp falls out of the window.
                sleep_for = self.window_seconds - (now - self._timestamps[0]) + 0.01
            await asyncio.sleep(max(sleep_for, 0.01))
