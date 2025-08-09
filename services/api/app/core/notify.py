import os, asyncio, json, httpx
WEBHOOK = os.getenv("SLACK_WEBHOOK","").strip()

async def send_slack_batch(events: list[dict]):
    if not WEBHOOK: return
    lines = []
    for e in events[:12]:
        src = (e.get("source") or "").upper()
        tkr = e.get("ticker") or "N/A"
        title = (e.get("title") or e.get("url") or "")[:140]
        url = e.get("url") or ""
        lines.append(f"[{src}] {tkr} — {title}\n{url}")
    text = "*Guerilla News — New events*\n" + "\n\n".join(lines)
    async with httpx.AsyncClient(timeout=5.0) as c:
        try:
            await c.post(WEBHOOK, json={"text": text})
        except Exception:
            pass
