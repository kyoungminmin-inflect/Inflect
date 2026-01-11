# src/ml/build_features.py
from datetime import date
from src.db.supabase_client import insert_features

FIN_KEYS = [
    "finance:revenue",
    "finance:op_income",
    "finance:net_income",
    "finance:assets",
    "finance:liabilities",
]

def build_features_for_company(sb, company_key: str, as_of: date | None = None) -> dict:
    # 최신 facts 가져오기(간단화: key별 최신 1개)
    feats = {}
    for k in FIN_KEYS:
        res = (sb.table("company_facts")
                 .select("fact_value_num, fact_value_text, as_of_date")
                 .eq("company_key", company_key)
                 .eq("fact_key", k)
                 .order("as_of_date", desc=True)
                 .limit(1)
                 .execute())
        rows = res.data or []
        if rows:
            feats[k] = rows[0].get("fact_value_num")

    # 파생 피처 예시: 부채비율
    assets = feats.get("finance:assets") or 0
    liab   = feats.get("finance:liabilities") or 0
    feats["ratio:debt_to_assets"] = (liab / assets) if assets else None

    return feats

def run_build_features(sb, as_of: date | None = None):
    companies = sb.table("companies").select("company_key").execute().data or []
    for c in companies:
        ck = c["company_key"]
        feats = build_features_for_company(sb, ck, as_of=as_of)
        insert_features(sb, ck, feats, as_of=as_of)

