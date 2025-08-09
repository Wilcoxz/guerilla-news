import asyncio, json, re
from datetime import datetime
from xml.etree import ElementTree as ET
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

TAG = "[RSS]"
STRIP = re.compile('<[^<]+?>')

def strip_html(s: str|None) -> str:
    if not s: return ""
    return STRIP.sub("", s)

def parse_feed(xml: str):
    # Best-effort RSS + Atom
    try:
        root = ET.fromstring(xml)
    except Exception:
        return []
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = []
    for it in root.findall(".//item"):  # RSS
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        desc = (it.findtext("description") or "").strip()
        items.append({"title": title, "link": link, "desc": desc})
    for it in root.findall(".//atom:entry", ns):  # Atom
        title = (it.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = it.find("atom:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        desc = (it.findtext("atom:summary", default="", namespaces=ns) or "").strip()
        items.append({"title": title, "link": link, "desc": desc})
    return items

async def run(add_event):
    http = Http()
    seen = set()
    try:
        while True:
            cfg = get_config()
            feeds = (cfg.get("rss") or [])
            for item in feeds:
                tkr = item.get("ticker")
                url = item.get("url")
                if not url: 
                    continue
                try:
                    r = await http.get(url)
                    if r.status_code in (200, 301, 302):
                        for post in parse_feed(r.text)[:5]:
                            title = strip_html(post.get("title"))
                            link  = post.get("link") or url
                            desc  = strip_html(post.get("desc"))
                            sig = signature("rss", link, title, desc)
                            if sig in seen:
                                continue
                            seen.add(sig)
                            row = {
                                "ticker": tkr, "source":"rss", "url": link,
                                "title": title, "snippet": desc[:180],
                                "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                                "meta": json.dumps({"feed": url})
                            }
                            await add_event(row, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(3)
    finally:
        await http.close()
