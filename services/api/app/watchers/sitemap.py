import asyncio, yaml, json, xml.etree.ElementTree as ET
from typing import Dict, Any
from datetime import datetime
from ..core.http import Http
from ..core.dedupe import signature

NS={"sm":"http://www.sitemaps.org/schemas/sitemap/0.9"}
TAG = "[SITEMAP]"

def parse(xml_text: str):
    try:
        root = ET.fromstring(xml_text)
        for url in root.findall("sm:url", NS):
            loc = url.find("sm:loc", NS)
            lm = url.find("sm:lastmod", NS)
            if loc is not None:
                yield loc.text, (lm.text if lm is not None else None)
    except Exception:
        return

async def run(add_event, cfg_path: str):
    http = Http()
    try:
        while True:
            with open(cfg_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            items = cfg.get("sitemaps", []) or []
            for it in items:
                tkr = it.get("ticker")
                sm = it.get("url")
                if not sm: continue
                try:
                    r = await http.get(sm)
                    if r.status_code != 200: continue
                    for loc, lm in parse(r.text) or []:
                        sig = signature("sitemap", loc, lm or "", "")
                        row = {
                            "ticker": tkr,
                            "source": "sitemap",
                            "url": loc,
                            "title": None,
                            "snippet": None,
                            "signature": sig,
                            "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"lastmod": lm}),
                        }
                        await add_event(row, notify=False, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(10)
    finally:
        await http.close()
