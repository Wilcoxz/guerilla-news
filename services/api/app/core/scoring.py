from datetime import datetime, timedelta
from typing import Dict, Any
from sqlalchemy.engine import Engine

CRED = {"cdn": 0.9, "cms": 0.8, "sitemap": 0.6, "halts": 0.7, "notices": 0.65}

def compute_fields(engine: Engine, ticker: str|None, source: str) -> Dict[str, Any]:
    now = datetime.utcnow()
    novelty = 0.0
    velocity = 0.0
    if ticker:
        with engine.begin() as conn:
            # novelty: any recent event in last 60 min?
            q1 = """
                SELECT COUNT(1) AS c FROM events
                WHERE ticker = :t AND datetime(seen_at) >= datetime(:cutoff)
            """
            cutoff = (now - timedelta(minutes=60)).isoformat()
            c = conn.exec_driver_sql(q1, {"t": ticker, "cutoff": cutoff}).first().c
            novelty = 1.0 if int(c) == 0 else 0.0

            # velocity: events in last 5m (any source)
            q2 = """
                SELECT COUNT(1) AS c FROM events
                WHERE ticker = :t AND datetime(seen_at) >= datetime(:cutoff)
            """
            cutoff2 = (now - timedelta(minutes=5)).isoformat()
            v = conn.exec_driver_sql(q2, {"t": ticker, "cutoff": cutoff2}).first().c
            # normalize: 0..1 with cap at 5
            velocity = min(float(v)/5.0, 1.0)

    credibility = CRED.get(source, 0.5)
    return {"novelty": novelty, "credibility": credibility, "velocity": velocity}
