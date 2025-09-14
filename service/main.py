import json
import sqlite3
from fastapi import FastAPI, Request

app = FastAPI(title="Bug Severity Service")

DB_PATH = "data/dev.sqlite3"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    with open("config/schema.sql", "r") as f:
        conn.executescript(f.read())
    conn.close()

init_db()  # ensure tables exist on startup


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/webhook/github")
async def webhook(request: Request):
    payload = await request.json()
    event_type = request.headers.get("X-GitHub-Event", "unknown")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (event_type, payload) VALUES (?, ?)",
        (event_type, json.dumps(payload)),
    )
    conn.commit()
    conn.close()

    print("=== GOT EVENT ===")
    print(payload)
    return {"ok": True}
