from datetime import datetime
from supabase import Client

def compute_scores(sb: Client, run_at: datetime) -> None:
    # 최신 feature 읽기(예시)
    feats = sb.table("features_hourly")\
        .select("*")\
        .eq("run_at", run_at.isoformat())\
        .execute().data or []

    emp_sum = None
    for f in feats:
        if f["metric"] == "abscs_emp_sum":
            emp_sum = f["value"]

    # 매우 단순한 룰 예시
    cashflow_signal = "NEUTRAL"
    gtm_fit = 50
    ops_risk = 50

    if emp_sum is not None:
        # emp_sum이 크면 시장 활력 높다고 가정(예시)
        gtm_fit = 60
        ops_risk = 45

    row = {
        "run_at": run_at.isoformat(),
        "scope": "market",
        "country": "US",
        "cashflow_signal": cashflow_signal,
        "gtm_fit_score": int(gtm_fit),
        "ops_legal_risk_score": int(ops_risk),
        "summary": "Hourly market snapshot (rule-based v0)",
        "details": {"emp_sum": emp_sum},
    }
    sb.table("scores_hourly").insert(row).execute()
