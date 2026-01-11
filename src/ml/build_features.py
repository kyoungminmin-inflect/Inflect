from datetime import datetime, timezone
from typing import List, Dict, Any
from supabase import Client

from src.normalize.normalize_census import normalize_abscs

def build_hourly_features(sb: Client, run_at: datetime) -> None:
    # 최근 raw census 이벤트 1개 가져오기
    res = sb.table("raw_events")\
        .select("*")\
        .eq("source", "census")\
        .order("fetched_at", desc=True)\
        .limit(1)\
        .execute()

    if not res.data:
        return

    payload = res.data[0]["payload"]
    rows = normalize_abscs(payload)

    # 아주 단순: EMP 합계 같은 지표 만들기(예시)
    emp_vals = []
    for row in rows:
        try:
            if row.get("EMP"):
                emp_vals.append(float(row["EMP"]))
        except:
            pass

    emp_sum = sum(emp_vals) if emp_vals else None

    feature_rows = [{
        "run_at": run_at.isoformat(),
        "country": "US",
        "metric": "abscs_emp_sum",
        "value": emp_sum,
        "meta": {"source": "census", "topic": "abscs_us_level"},
    }]

    sb.table("features_hourly").insert(feature_rows).execute()
