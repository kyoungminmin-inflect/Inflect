# src/db/supabase_client.py
import os, json, hashlib
from datetime import date
from supabase import create_client

def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Supabase env missing: SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

def _hash_payload(payload: dict) -> str:
    s = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def insert_raw_event(sb, source: str, dataset: str, params: dict, payload: dict, as_of: date | None = None):
    row = {
        "source": source,
        "dataset": dataset,
        "params": params or {},
        "payload": payload,
        "payload_hash": _hash_payload(payload),
        "as_of_date": as_of.isoformat() if as_of else None
    }
    # upsert 흉내: unique index 충돌 시 에러날 수 있어 try/except로 스킵
    try:
        sb.table("raw_api_events").insert(row).execute()
    except Exception as e:
        # 중복이면 무시, 다른 에러면 올리기
        msg = str(e)
        if "duplicate key" in msg or "raw_api_events_dedupe" in msg:
            return
        raise

def upsert_company_fact(sb, company_key: str, fact_key: str,
                        value_text=None, value_num=None, value_json=None,
                        source="pipeline", as_of: date | None = None, confidence: float = 1.0):
    row = {
        "company_key": company_key,
        "fact_key": fact_key,
        "fact_value_text": value_text,
        "fact_value_num": value_num,
        "fact_value_json": value_json,
        "source": source,
        "as_of_date": as_of.isoformat() if as_of else None,
        "confidence": confidence
    }
    sb.table("company_facts").insert(row).execute()

def insert_market_indicator(sb, indicator_key: str, value_num=None, value_json=None,
                           source="pipeline", as_of: date | None = None):
    row = {
        "indicator_key": indicator_key,
        "as_of_date": as_of.isoformat() if as_of else None,
        "value_num": value_num,
        "value_json": value_json,
        "source": source
    }
    sb.table("market_indicators").insert(row).execute()

def insert_features(sb, company_key: str, features: dict, as_of: date | None = None):
    sb.table("ml_features").insert({
        "company_key": company_key,
        "as_of_date": as_of.isoformat() if as_of else None,
        "features": features
    }).execute()

def insert_scores(sb, company_key: str, scores: dict, signals: dict | None = None,
                  model_version: str | None = None, as_of: date | None = None):
    sb.table("diagnostic_scores").insert({
        "company_key": company_key,
        "as_of_date": as_of.isoformat() if as_of else None,
        "scores": scores,
        "signals": signals or {},
        "model_version": model_version,
        "source": "pipeline"
    }).execute()

def insert_report(sb, report_type: str, content_md: str, scope: dict, as_of: date | None = None):
    sb.table("reports").insert({
        "report_type": report_type,
        "as_of_date": as_of.isoformat() if as_of else None,
        "scope": scope,
        "content_md": content_md
    }).execute()

