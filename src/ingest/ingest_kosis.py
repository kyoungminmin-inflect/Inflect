import os, requests
from typing import Dict, Any

def fetch_kosis_example() -> Dict[str, Any]:
    key = os.getenv("KOSIS_API_KEY")
    if not key:
        raise RuntimeError("Missing KOSIS_API_KEY")

    # TODO: 네가 실제로 쓰는 KOSIS 엔드포인트/파라미터로 교체
    url = "https://kosis.kr/openapi/statisticsData.do"
    params = {
        "method": "getList",
        "apiKey": key,
        "format": "json",
        "jsonVD": "Y",
        # "orgId": "...",
        # "tblId": "...",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return {"endpoint": url, "params": params, "data": r.json()}
