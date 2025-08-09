import asyncio, json
from datetime import datetime
from bs4 import BeautifulSoup
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

DEFAULT_PAGES = {
  "NYSE": "https://www.nyse.com/market-status/notices"
}
TAG="[NOTICES]"

async def run(add_event):
    http = Http()
    seen_rows: set[str] = set()
    try:
        while True:
            cfg = get_config()
            pages = dict(DEFAULT_PAGES)
            for kv in (cfg.get("notices") or []):
                if kv.get("name") and kv.get("url"):
                    pages[kv["name"]] = kv["url"]
            for name, url in pages.items():
                try:
                    r = await http.get(url)
                    if r.status_code == 304: continue
                    if r.status_code != 200: continue
                    soup = BeautifulSoup(r.text, "lxml")
                    for tr in soup.select("table tr"):
                        cells = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
                        if len(cells) < 3: continue
                        row_key = f"{name}|{'|'.join(cells)}"
                        sig = signature("notices", f"{url}#{hash(row_key)}", "|".join(cells[:4]), "")
                        if sig in seen_rows: continue
                        seen_rows.add(sig)
                        payload = {
                            "ticker": None, "source":"notices", "url": url,
                            "title": f"{name} notice: {' | '.join(cells[:4])[:200]}",
                            "snippet": "",
                            "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"exchange": name, "cells": cells[:6]})
                        }
                        await add_event(payload, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(5)
    finally:
        await http.close()
