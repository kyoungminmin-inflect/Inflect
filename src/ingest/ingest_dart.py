import os, requests
from typing import Dict, Any

def fetch_fnltt_singl_acnt_all(corp_code: str, bsns_year: str, reprt_code: str="11011") -> Dict[str, Any]:
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
    return {"endpoint": url, "params": params, "data": r.json()}
