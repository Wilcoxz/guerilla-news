import os, asyncio, time
from typing import List, Dict
import httpx

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")

async def send_slack_batch(events: List[Dict]):
    if not SLACK_WEBHOOK or not events:
        return
    lines = []
    for e in events[:10]:
        t = e.get("ticker") or "N/A"
        src = e.get("source","?")
        url = e.get("url","")
        title = (e.get("title") or "")[:110]
        display = title if title else url
        lines.append(f"[{src}] {t} — {display} — {url}")
    text = "*Guerilla News — new events*\n" + "\n".join(lines)
    async with httpx.AsyncClient(timeout=5.0) as c:
        await c.post(SLACK_WEBHOOK, json={"text": text})
