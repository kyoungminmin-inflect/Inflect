import os, requests
from typing import Dict, Any, Optional

BASE = "https://api.census.gov/data"

def fetch_abscs_us_level() -> Dict[str, Any]:
    key = os.getenv("CENSUS_API_KEY")
    if not key:
        raise RuntimeError("Missing CENSUS_API_KEY")

    # 예시: US 전체 Employer-firm 요약 일부
    url = f"{BASE}/2023/abscs"
    params = {
        "get": "NAME,NAICS2022,EMP,PAYANN",
        "for": "us:*",
        "key": key,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return {"endpoint": url, "params": params, "data": r.json()}
