import asyncio, yaml, re, json
from typing import Dict, Any, List
from datetime import datetime
from ..core.http import Http
from ..core.dedupe import signature

TAG = "[CMS]"

def strip_html(s: str) -> str:
    return re.sub('<[^<]+?>', '', s or '')

async def run(add_event, cfg_path: str):
    http = Http()
    try:
        while True:
            with open(cfg_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            items = cfg.get("wordpress", []) or []
            for it in items:
                tkr = it.get("ticker")
                url = it.get("url")
                if not url: continue
                try:
                    r = await http.get(url)
                    if r.status_code != 200: continue
                    data = r.json()
                    post = data[0] if isinstance(data, list) and data else data
                    title = strip_html((post.get("title") or {}).get("rendered",""))
                    excerpt = strip_html((post.get("excerpt") or {}).get("rendered",""))
                    link = post.get("link") or (post.get("guid") or {}).get("rendered") or url
                    sig = signature("cms", link, title, excerpt)
                    row = {
                        "ticker": tkr,
                        "source": "cms",
                        "url": link,
                        "title": title,
                        "snippet": excerpt,
                        "signature": sig,
                        "seen_at": datetime.utcnow().isoformat(),
                        "meta": json.dumps({"raw": post}),
                    }
                    await add_event(row, notify=True, log_prefix=TAG)
                except Exception:
                    # keep running; this worker must not crash
                    pass
            await asyncio.sleep(2)
    finally:
        await http.close()
