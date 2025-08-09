import random, asyncio, time
from typing import Optional, Dict, Any
import httpx

DEFAULT_HEADERS = {"User-Agent": "guerilla-news-bot/1.0"}

def _jitter(base: float) -> float:
    return base * (0.9 + random.random()*0.2)

class Http:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0), headers=DEFAULT_HEADERS, follow_redirects=True)

    async def get(self, url: str, headers: Optional[Dict[str,str]]=None, retries: int=3) -> httpx.Response:
        backoff = 0.4
        last_exc = None
        for _ in range(retries):
            try:
                return await self.client.get(url, headers=headers)
            except Exception as e:
                last_exc = e
                await asyncio.sleep(_jitter(backoff))
                backoff = min(backoff*2, 6.4)
        raise last_exc

    async def head(self, url: str, headers: Optional[Dict[str,str]]=None, retries: int=3) -> httpx.Response:
        backoff = 0.4
        last_exc = None
        for _ in range(retries):
            try:
                return await self.client.head(url, headers=headers)
            except Exception as e:
                last_exc = e
                await asyncio.sleep(_jitter(backoff))
                backoff = min(backoff*2, 6.4)
        raise last_exc

    async def close(self):
        await self.client.aclose()
