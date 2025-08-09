import asyncio, yaml, json
from datetime import datetime
from bs4 import BeautifulSoup
from ..core.http import Http
from ..core.dedupe import signature

TAG = "[NOTICES]"

# seed with an example; add more via config later if you prefer
NOTICE_PAGES = {
  "NYSE": "https://www.nyse.com/market-status/notices"
}

async def run(add_event, cfg_path: str):
    http = Http()
    seen_rows: set[str] = set()
    try:
        while True:
            for name, url in NOTICE_PAGES.items():
                try:
                    r = await http.get(url)
                    if r.status_code == 304:
                        continue
                    if r.status_code != 200:
                        continue
                    soup = BeautifulSoup(r.text, "lxml")
                    # generic table grab; adjust selector if page changes
                    for tr in soup.select("table tr"):
                        cells = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
                        if len(cells) < 3:
                            continue
                        row_key = f"{name}|{'|'.join(cells)}"
                        if row_key in seen_rows:
                            continue
                        seen_rows.add(row_key)
                        title = " | ".join(cells[:4])[:200]
                        sig = signature("notices", f"{url}#{hash(row_key)}", title, "")
                        payload = {
                            "ticker": None,
                            "source": "notices",
                            "url": url,
                            "title": f"{name} notice: {title}",
                            "snippet": "",
                            "signature": sig,
                            "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"exchange": name, "cells": cells[:6]}),
                        }
                        await add_event(payload, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(5)
    finally:
        await http.close()
