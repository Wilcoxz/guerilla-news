import asyncio, csv, io, json
from datetime import datetime
from ..core.http import Http
from ..core.config import get_config
from ..core.dedupe import signature

TAG="[HALTS]"

async def run(add_event):
    http = Http()
    seen = set()
    try:
        while True:
            cfg = get_config()
            for item in (cfg.get("halts") or []):
                url = item.get("url"); name=item.get("name","nasdaq")
                if not url: continue
                try:
                    r = await http.get(url)
                    if r.status_code == 304: continue
                    if r.status_code != 200: continue
                    text = r.text
                    # nasdaq txt is pipe-separated; try csv first, fallback to manual parse
                    rows = []
                    try:
                        f = io.StringIO(text)
                        reader = csv.DictReader(f, delimiter='|')
                        rows = list(reader)
                    except Exception:
                        pass
                    for rowd in rows[:200]:
                        sym = (rowd.get("Symbol") or rowd.get("Issue Symbol") or "").strip().upper()
                        if not sym: continue
                        reason = (rowd.get("Reason Code") or rowd.get("Reason") or "").strip()
                        ts = (rowd.get("Halt Time") or rowd.get("Halt Date") or rowd.get("Halt Time (ET)") or "").strip()
                        key = f"{sym}|{reason}|{ts}"
                        sig = signature("halts", url, key, "")
                        if sig in seen: continue
                        seen.add(sig)
                        payload = {
                            "ticker": sym, "source":"halts", "url": url,
                            "title": f"Halt {sym} ({reason})",
                            "snippet": ts,
                            "signature": sig, "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps({"feed": name, "raw": rowd})
                        }
                        await add_event(payload, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(10)
    finally:
        await http.close()
