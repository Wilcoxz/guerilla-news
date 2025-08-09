import asyncio, yaml, csv, io, json
from datetime import datetime
from ..core.http import Http
from ..core.dedupe import signature

TAG = "[HALTS]"

def parse_nasdaq_txt(text: str):
    # file is pipe- or tab-aligned; split on whitespace preserving columns
    lines = [ln for ln in text.splitlines() if ln.strip()]
    # try CSV with delimiter=|, else fallback
    try:
        for row in csv.DictReader(io.StringIO(text), delimiter='|'):
            yield row
        return
    except Exception:
        pass
    # fallback: simple split
    headers = lines[0].split()
    for ln in lines[1:]:
        parts = ln.split()
        if len(parts) >= len(headers):
            yield dict(zip(headers, parts))

async def run(add_event, cfg_path: str):
    http = Http()
    try:
        while True:
            with open(cfg_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            items = cfg.get("halts", []) or []
            for it in items:
                url = it.get("url"); name = it.get("name","halts")
                if not url: continue
                try:
                    r = await http.get(url)
                    if r.status_code != 200: continue
                    for row in parse_nasdaq_txt(r.text) or []:
                        sym = (row.get("Symbol") or row.get("IssueSymbol") or "").strip()
                        reason = (row.get("Reason Code") or row.get("Reason") or "")
                        href = url
                        if not sym: continue
                        sig = signature("halts", f"{href}#{sym}", reason, "")
                        payload = {
                            "ticker": sym,
                            "source": "halts",
                            "url": href,
                            "title": f"Halt: {sym} ({reason})",
                            "snippet": "",
                            "signature": sig,
                            "seen_at": datetime.utcnow().isoformat(),
                            "meta": json.dumps(row),
                        }
                        await add_event(payload, notify=True, log_prefix=TAG)
                except Exception:
                    pass
            await asyncio.sleep(15)
    finally:
        await http.close()
