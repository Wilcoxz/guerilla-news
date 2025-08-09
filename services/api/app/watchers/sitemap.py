import asyncio, json, xml.etree.ElementTree as ET
from datetime import datetime
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

NS={"sm":"http://www.sitemaps.org/schemas/sitemap/0.9"}
TAG="[SITEMAP]"

def parse(xml_text: str):
    root = ET.fromstring(xml_text)
    for url in root.findall("sm:url", NS):
        loc = url.find("sm:loc", NS)
        lm = url.find("sm:lastmod", NS)
        if loc is not None:
            yield loc.text, (lm.text if lm is not None else None)

async def run(add_event):
    http = Http()
    seen = set()
    try:
        while True:
            cfg = get_config()
            for item in (cfg.get("sitemaps") or []):
                tkr = item.get("ticker")
                url = item.get("url")
                if not url: continue
                try:
                    r = await http.get(url)
                    if r.status_code == 304: continue
                    if r.status_code != 200: continue
                    for loc,lm in parse(r.text):
                        sig = signature("sitemap", loc, lm or "", "")
                        if sig in seen: continue
                        seen.add(sig)
                        row = {
                            "ticker": tkr, "source":"sitemap", "url": loc,
                            "title": None, "snippet": None,
                            "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"lastmod": lm, "sitemap": url})
                        }
                        await add_event(row, notify=False, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(10)
    finally:
        await http.close()
