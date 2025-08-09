import asyncio, yaml, json
from datetime import datetime
from ..core.http import Http
from ..core.dedupe import signature

TAG = "[CDN]"

async def run(add_event, cfg_path: str):
    http = Http()
    try:
        while True:
            with open(cfg_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            items = cfg.get("cdn_candidates", []) or []
            for it in items:
                tkr = it.get("ticker")
                urls = it.get("urls") or []
                for url in urls:
                    try:
                        r = await http.head(url)
                        if r.status_code != 200: 
                            continue
                        etag = r.headers.get("ETag")
                        lm = r.headers.get("Last-Modified")
                        cl = r.headers.get("Content-Length")
                        sig = signature("cdn", url, etag or "", lm or (cl or ""))
                        row = {
                            "ticker": tkr,
                            "source": "cdn",
                            "url": url,
                            "title": None,
                            "snippet": None,
                            "signature": sig,
                            "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"etag": etag, "last_modified": lm, "len": cl}),
                        }
                        await add_event(row, notify=True, log_prefix=TAG)
                    except Exception:
                        pass
            await asyncio.sleep(3)
    finally:
        await http.close()
