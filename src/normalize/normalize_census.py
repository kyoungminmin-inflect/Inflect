# src/normalize/normalize_census.py
from datetime import date
from src.db.supabase_client import insert_market_indicator

def normalize_abscs_us_level(sb, payload: dict):
    """
    payload["data"] 형식: 2D array (헤더행 + 데이터행들)
    """
    rows = payload["data"]
    header = rows[0]
    for row in rows[1:]:
        rec = dict(zip(header, row))
        naics = rec.get("NAICS2022")
        year  = rec.get("YEAR")
        emp   = rec.get("EMP")
        pay   = rec.get("PAYANN")

        if not (naics and year):
            continue

        as_of = date(int(year), 12, 31)
        indicator_key = f"CENSUS:abscs:naics:{naics}"
        insert_market_indicator(
            sb,
            indicator_key=indicator_key,
            value_json={"emp": emp, "payann": pay, "naics_label": rec.get("NAICS2022_LABEL")},
            source="CENSUS",
            as_of=as_of
        )

def normalize_bds_us_level(sb, payload: dict):
    rows = payload["data"]
    header = rows[0]
    for row in rows[1:]:
        rec = dict(zip(header, row))
        year = rec.get("YEAR")
        if not year:
            continue
        as_of = date(int(year), 12, 31)
        indicator_key = "CENSUS:bds:us"
        insert_market_indicator(sb, indicator_key, value_json=rec, source="CENSUS", as_of=as_of)

