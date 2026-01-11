# src/normalize/normalize_dart.py
from datetime import date
from src.db.supabase_client import upsert_company_fact

# DART 응답에서 계정명/금액을 표준 키로 매핑 (MVP는 최소만)
ACCOUNT_MAP = {
    "매출액": "revenue",
    "영업이익": "op_income",
    "당기순이익": "net_income",
    "자산총계": "assets",
    "부채총계": "liabilities",
}

def normalize_fnltt(sb, company_key: str, payload: dict, bsns_year: str, reprt_code: str):
    data = payload["data"]
    if data.get("status") != "000":
        # DART 오류면 스킵
        return

    as_of = date(int(bsns_year), 12, 31)  # 간단화(분기면 더 정교화 가능)
    for item in data.get("list", []):
        acc = item.get("account_nm")
        amt = item.get("thstrm_amount")  # 문자열
        if not acc or amt is None:
            continue

        std_key = ACCOUNT_MAP.get(acc)
        if not std_key:
            continue

        # 숫자 변환
        try:
            num = float(str(amt).replace(",", ""))
        except:
            num = None

        upsert_company_fact(
            sb,
            company_key=company_key,
            fact_key=f"finance:{std_key}",
            value_num=num,
            value_text=str(amt),
            value_json={"account_nm": acc, "reprt_code": reprt_code},
            source="DART",
            as_of=as_of,
            confidence=0.9
        )

