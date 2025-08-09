import asyncio, json, re
from datetime import datetime
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

TAG = "[CMS]"
STRIP = re.compile('<[^<]+?>')

def strip(s: str|None) -> str:
    if not s: return ""
    return STRIP.sub("", s)

async def run(add_event):
    http = Http()
    try:
        while True:
            cfg = get_config()
            wp_list = (cfg.get("wordpress") or [])
            for item in wp_list:
                tkr = item.get("ticker")
                url = item.get("url")
                if not url: continue
                try:
                    r = await http.get(url)
                    if r.status_code == 304: continue
                    if r.status_code != 200: continue
                    data = r.json()
                    post = data[0] if isinstance(data, list) and data else data
                    title = strip((post.get("title") or {}).get("rendered",""))
                    excerpt = strip((post.get("excerpt") or {}).get("rendered",""))
                    link = post.get("link") or (post.get("guid") or {}).get("rendered") or url
                    sig = signature("cms", link, title, excerpt)
                    row = {
                        "ticker": tkr, "source":"cms", "url": link,
                        "title": title, "snippet": excerpt,
                        "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                        "meta": json.dumps({"endpoint": url})
                    }
                    await add_event(row, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(2)
    finally:
        await http.close()
