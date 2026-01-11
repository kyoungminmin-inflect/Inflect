# src/ingest/ingest_kosis.py
import os, requests

def fetch_kosis(params: dict):
    api_key = os.getenv("KOSIS_API_KEY")
    ua = os.getenv("USER_AGENT", "InflectBot/1.0 (contact: dev@inflect.local)")
    if not api_key:
        raise RuntimeError("Missing KOSIS_API_KEY")

    # 너가 사용하는 KOSIS REST endpoint로 바꿔 넣으세요.
    # 예: https://kosis.kr/openapi/... 형태
    base_url = os.getenv("KOSIS_BASE_URL", "https://kosis.kr/openapi/Param/statisticsParameterData.do")

    merged = dict(params)
    merged["apiKey"] = api_key

    r = requests.get(base_url, params=merged, headers={"User-Agent": ua}, timeout=30)
    r.raise_for_status()

    # KOSIS는 json/xml 등 응답형이 다양 -> 우선 텍스트로 받고, json이면 json으로
    try:
        data = r.json()
    except Exception:
        data = {"raw_text": r.text[:20000]}
    return {"endpoint": "kosis", "params": merged, "data": data}

