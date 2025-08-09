import asyncio, os, json
from datetime import datetime
from urllib.parse import urlparse
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from jinja2 import Environment, FileSystemLoader
from .core.config import configure, get_config, reload_config
from .core.notify import send_slack_batch
from .watchers import cms, sitemap, cdn, halts, notices

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./events.sqlite")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")
ALLOWED = {h.strip().lower() for h in os.getenv("ALLOWED_DOMAINS","").split(",") if h.strip()}

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True, autocommit=False, autoflush=False)

with engine.begin() as conn:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT, source TEXT, url TEXT, title TEXT, snippet TEXT,
      signature TEXT UNIQUE, seen_at TEXT, meta TEXT
    );
    """)

CFG_PATH = "services/api/app/config/watchlist.yaml"

app = FastAPI(title="Guerilla News")
app.mount("/static", StaticFiles(directory="services/api/app/static"), name="static")
env = Environment(loader=FileSystemLoader("services/api/app/static"))

def insert_event(row: dict) -> bool:
    """Returns True if inserted, False if duplicate (UNIQUE signature)."""
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("""
            INSERT OR IGNORE INTO events(ticker,source,url,title,snippet,signature,seen_at,meta)
            VALUES(:ticker,:source,:url,:title,:snippet,:signature,:seen_at,:meta)
            """, row)
            # Check if it exists now
            exists = conn.exec_driver_sql("SELECT 1 FROM events WHERE signature=:s", {"s": row["signature"]}).first()
            return bool(exists)
    except Exception:
        return False

async def add_event_async(row: dict, notify: bool=False, log_prefix: str=""):
    # host allowlist (if provided)
    if ALLOWED:
        host = urlparse(row.get("url") or "").hostname or ""
        if host.lower() not in ALLOWED:
            print("[SKIP host]", host)
            return
    inserted = insert_event(row)
    if inserted:
        print(f"{log_prefix} NEW", row.get("ticker"), "->", row.get("url"))
        if notify and SLACK_WEBHOOK:
            await send_slack_batch([row])

@app.get("/health")
def health(): 
    return {"ok": True, "time": datetime.utcnow().isoformat()}

@app.get("/config")
def read_config():
    return JSONResponse(get_config())

@app.post("/config/reload")
def force_reload():
    cfg = reload_config()
    return JSONResponse({"ok": True, "reloaded": True, "keys": list(cfg.keys())})

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

# Optional demo generator (leave on for now)
async def demo_generator():
    import hashlib, random
    while True:
        title = f"Demo ping {datetime.utcnow().strftime('%H:%M:%S')}"
        sig = hashlib.sha1(title.encode()).hexdigest()
        await add_event_async({
          "ticker": random.choice(["AAPL","MSFT","NVDA","META"]),
          "source": "demo",
          "url": "https://example.com",
          "title": title,
          "snippet": "replace me with real sources",
          "signature": sig,
          "seen_at": datetime.utcnow().isoformat(),
          "meta": json.dumps({"demo": True}),
        }, notify=False, log_prefix="[DEMO]")
        await asyncio.sleep(20)

@app.on_event("startup")
async def startup():
    configure(CFG_PATH)
    loop = asyncio.get_event_loop()
    loop.create_task(demo_generator())
    loop.create_task(cms.run(add_event_async))
    loop.create_task(sitemap.run(add_event_async))
    loop.create_task(cdn.run(add_event_async))
    loop.create_task(halts.run(add_event_async))
    loop.create_task(notices.run(add_event_async))
