from datetime import date, datetime, timedelta, timezone
from src.db.supabase_client import get_supabase

def build_daily_md(rows):
    lines = []
    lines.append(f"# Daily Market Report")
    lines.append("")
    for r in rows:
        lines.append(f"## {r.get('country','')}/{r.get('scope','')}")
        lines.append(f"- GTM Fit: **{r.get('gtm_fit_score')}**")
        lines.append(f"- Ops/Legal Risk: **{r.get('ops_legal_risk_score')}**")
        lines.append(f"- Cashflow Signal: **{r.get('cashflow_signal')}**")
        lines.append(f"- Summary: {r.get('summary')}")
        lines.append("")
    return "\n".join(lines)

def main():
    sb = get_supabase()
    # UTC 기준 전일
    today = datetime.now(timezone.utc).date()
    report_date = today - timedelta(days=1)

    start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    res = sb.table("scores_hourly")\
        .select("*")\
        .gte("run_at", start.isoformat())\
        .lt("run_at", end.isoformat())\
        .execute()

    rows = res.data or []
    md = build_daily_md(rows)

    sb.table("reports_daily").upsert({
        "report_date": str(report_date),
        "country": "GLOBAL",
        "title": f"Daily Report {report_date}",
        "body_md": md,
        "highlights": {"n_scores": len(rows)}
    }, on_conflict="report_date,country").execute()

    print("OK - daily report saved:", report_date)

if __name__ == "__main__":
    main()
