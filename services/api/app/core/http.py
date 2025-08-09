import random, asyncio
from typing import Optional, Dict
import httpx

DEFAULT_HEADERS = {"User-Agent": "guerilla-news-bot/1.1"}

# simple in-process cache: url -> {"etag":..., "last_modified":...}
_http_cache: dict[str, dict[str, str]] = {}

def _jitter(base: float) -> float:
    return base * (0.9 + random.random()*0.2)

def _with_conditional_headers(url: str, headers: Optional[Dict[str,str]]) -> Dict[str,str]:
    h = dict(DEFAULT_HEADERS)
    if headers: h.update(headers)
    ent = _http_cache.get(url)
    if ent:
        if ent.get("etag"): h["If-None-Match"] = ent["etag"]
        if ent.get("last_modified"): h["If-Modified-Since"] = ent["last_modified"]
    return h

def _store_conditionals(url: str, r: httpx.Response) -> None:
    etag = r.headers.get("ETag")
    lm = r.headers.get("Last-Modified")
    if etag or lm:
        _http_cache[url] = {"etag": etag or "", "last_modified": lm or ""}

class Http:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0, connect=3.0),
            follow_redirects=True,
            headers=DEFAULT_HEADERS
        )

    async def get(self, url: str, headers: Optional[Dict[str,str]]=None, retries: int=3) -> httpx.Response:
        backoff = 0.4
        last_exc = None
        for _ in range(retries):
            try:
                r = await self.client.get(url, headers=_with_conditional_headers(url, headers))
                if r.status_code != 304:
                    _store_conditionals(url, r)
                return r
            except Exception as e:
                last_exc = e
                await asyncio.sleep(_jitter(backoff)); backoff = min(backoff*2, 6.4)
        raise last_exc

    async def head(self, url: str, headers: Optional[Dict[str,str]]=None, retries: int=3) -> httpx.Response:
        backoff = 0.4
        last_exc = None
        for _ in range(retries):
            try:
                r = await self.client.head(url, headers=_with_conditional_headers(url, headers))
                if r.status_code != 304:
                    _store_conditionals(url, r)
                return r
            except Exception as e:
                last_exc = e
                await asyncio.sleep(_jitter(backoff)); backoff = min(backoff*2, 6.4)
        raise last_exc

    async def close(self):
        await self.client.aclose()
