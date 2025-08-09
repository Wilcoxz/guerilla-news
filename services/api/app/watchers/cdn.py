import asyncio, json
from datetime import datetime
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

TAG = "[CDN]"

async def run(add_event):
    http = Http()
    try:
        while True:
            cfg = get_config()
            for item in (cfg.get("cdn_candidates") or []):
                tkr = item.get("ticker")
                urls = item.get("urls") or []
                for url in urls:
                    try:
                        r = await http.head(url)
                        if r.status_code == 304: 
                            continue
                        if r.status_code != 200:
                            continue
                        etag=r.headers.get("ETag"); lm=r.headers.get("Last-Modified"); cl=r.headers.get("Content-Length")
                        sig = signature("cdn", url, etag or "", (lm or "") + (cl or ""))
                        row = {
                            "ticker": tkr, "source":"cdn", "url": url,
                            "title": None, "snippet": None,
                            "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"etag":etag,"last_modified":lm,"len":cl})
                        }
                        await add_event(row, notify=True, log_prefix=TAG)
                    except Exception:
                        pass
            await asyncio.sleep(2)
    finally:
        await http.close()
