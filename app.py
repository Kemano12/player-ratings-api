import os
from typing import Any, Dict
import json

from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import create_engine, text

API_TOKEN = os.environ["API_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
app = FastAPI()

def auth(authorization: str | None):
    if not authorization or authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.on_event("startup")
def startup():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS submissions (
          match_key TEXT PRIMARY KEY,
          payload JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """))

@app.put("/api/submissions")
def upsert_submission(body: Dict[str, Any], authorization: str | None = Header(default=None)):
    auth(authorization)

    match_key = body.get("match_key")
    if not match_key:
        raise HTTPException(status_code=400, detail="match_key is required")

    payload_json = json.dumps(body)   # convert dict → JSON string

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO submissions (match_key, payload)
            VALUES (:match_key, CAST(:payload AS JSONB))
            ON CONFLICT (match_key)
            DO UPDATE SET payload = EXCLUDED.payload, updated_at = now();
            """),
            {
                "match_key": match_key,
                "payload": payload_json
            },
        )

    return {"ok": True, "match_key": match_key}

@app.get("/api/submissions/{match_key}")
def get_submission(match_key: str, authorization: str | None = Header(default=None)):
    auth(authorization)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT payload FROM submissions WHERE match_key = :k"),
            {"k": match_key},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    payload = row["payload"]
    payload["found"] = True
    return payload
