# src/db/supabase_client.py
import os
from typing import Any, Dict, Optional
from supabase import create_client, Client


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def insert_raw_event(
    sb: Client,
    *,
    source: str,
    topic: str,
    payload: Dict[str, Any],
    asset: Optional[str] = None,
    symbol: Optional[str] = None,
    company_id: Optional[str] = None,
) -> None:
    row = {
        "source": source,
        "topic": topic,
        "asset": asset,
        "symbol": symbol,
        "company_id": company_id,
        "payload": payload,
    }
    sb.table("raw_events").insert(row).execute()
