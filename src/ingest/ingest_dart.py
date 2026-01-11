# src/ingest/ingest_dart.py
import os
import requests

def fetch_fnltt_singl_acnt_all(corp_code: str, bsns_year: str, reprt_code: str = "11011") -> dict:
    """
    DART 단일회사 전체 재무제표 조회
    reprt_code:
      11011 사업보고서(연간)
      11012 반기보고서
      11013 1분기보고서
      11014 3분기보고서
    """
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise RuntimeError("Missing DART_API_KEY")

    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return {"endpoint": "fnlttSinglAcntAll", "params": params, "data": r.json()}
