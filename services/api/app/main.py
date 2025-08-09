import os, json, asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from jinja2 import Environment, FileSystemLoader

from .core.notify import send_slack_batch
from .watchers import cms, sitemap, cdn, halts

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./events.sqlite")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")
CFG_PATH = "services/api/app/config/watchlist.yaml"

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True, autocommit=False, autoflush=False)

# schema
with engine.begin() as conn:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT, source TEXT, url TEXT, title TEXT, snippet TEXT,
      signature TEXT UNIQUE, seen_at TEXT, meta TEXT
    );
    """)
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_seen_at ON events(seen_at DESC);")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ticker_seen ON events(ticker, seen_at DESC);")

app = FastAPI(title="Guerilla News")
app.mount("/static", StaticFiles(directory="services/api/app/static"), name="static")
env = Environment(loader=FileSystemLoader("services/api/app/static"))

async def add_event(row: Dict[str,Any], notify: bool=False, log_prefix: str=""):
    # insert-or-ignore by UNIQUE(signature)
    inserted = False
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql("""
            INSERT OR IGNORE INTO events(ticker,source,url,title,snippet,signature,seen_at,meta)
            VALUES(:ticker,:source,:url,:title,:snippet,:signature,:seen_at,:meta)
            """, row)
            # detect if inserted by checking rowid
            res = conn.exec_driver_sql("SELECT changes() AS c").first()
            inserted = bool(res and res.c == 1)
        except Exception:
            inserted = False
    if inserted:
        if log_prefix:
            print(log_prefix, "NEW", row.get("ticker"), "->", row.get("url"))
        if notify:
            # batch to avoid Slack floods
            app.state.notify_buffer.append(row)

@app.on_event("startup")
async def startup():
    app.state.notify_buffer = []
    # notifier task
    async def notifier():
        while True:
            if app.state.notify_buffer:
                batch = app.state.notify_buffer[:]
                app.state.notify_buffer.clear()
                try:
                    await send_slack_batch(batch)
                except Exception:
                    pass
            await asyncio.sleep(2)

    # (optional) demo generator
    # async def demo():
    #     import hashlib, random
    #     while True:
    #         title = f"Demo ping {datetime.utcnow().strftime('%H:%M:%S')}"
    #         sig = hashlib.sha1(title.encode()).hexdigest()
    #         row = {
    #           "ticker": random.choice(["AAPL","MSFT","NVDA","META"]),
    #           "source": "demo",
    #           "url": "https://example.com",
    #           "title": title,
    #           "snippet": "replace with real sources soon",
    #           "signature": sig,
    #           "seen_at": datetime.utcnow().isoformat(),
    #           "meta": json.dumps({"demo": True}),
    #         }
    #         await add_event(row, notify=False, log_prefix="[DEMO]")
    #         await asyncio.sleep(20)

    loop = asyncio.get_event_loop()
    loop.create_task(notifier())
    loop.create_task(cms.run(add_event, CFG_PATH))
    loop.create_task(sitemap.run(add_event, CFG_PATH))
    loop.create_task(cdn.run(add_event, CFG_PATH))
    loop.create_task(halts.run(add_event, CFG_PATH))
    # loop.create_task(demo())

@app.get("/health")
def health(): 
    return {"ok": True, "time": datetime.utcnow().isoformat()}

@app.get("/events")
def events(limit: int = 200, source: str | None = None, ticker: str | None = None):
    q = "SELECT * FROM events"
    cond, params = [], {}
    if source: cond.append("source=:source"); params["source"]=source
    if ticker: cond.append("ticker=:ticker"); params["ticker"]=ticker
    if cond: q += " WHERE " + " AND ".join(cond)
    q += " ORDER BY datetime(seen_at) DESC LIMIT :limit"; params["limit"]=limit
    with engine.begin() as conn:
        rows = [dict(r._mapping) for r in conn.exec_driver_sql(q, params)]
    return JSONResponse(rows)

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    tpl = env.get_template("index.html")
    return tpl.render()
