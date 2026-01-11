# src/run_hourly_train.py
from datetime import date
from src.db.supabase_client import get_supabase, insert_raw_event
from src.ingest.ingest_census import fetch_abscs_us_level, fetch_bds_timeseries_example
from src.ingest.ingest_dart import fetch_fnltt_singl_acnt_all
from src.ingest.ingest_kosis import fetch_kosis

from src.normalize.normalize_census import normalize_abscs_us_level, normalize_bds_us_level
from src.normalize.normalize_dart import normalize_fnltt
from src.normalize.normalize_kosis import normalize_kosis_market

from src.ml.build_features import run_build_features
from src.ml.train import run_train
from src.ml.predict import run_predict

def main():
    sb = get_supabase()
    today = date.today()

    # 1) Census ingest + raw save + normalize
    census_abscs = fetch_abscs_us_level()
    insert_raw_event(sb, "CENSUS", "abscs", census_abscs["params"], {"data": census_abscs["data"]}, as_of=today)
    normalize_abscs_us_level(sb, {"data": census_abscs["data"]})

    census_bds = fetch_bds_timeseries_example()
    insert_raw_event(sb, "CENSUS", "bds", census_bds["params"], {"data": census_bds["data"]}, as_of=today)
    normalize_bds_us_level(sb, {"data": census_bds["data"]})

    # 2) DART ingest (companies에 dart_corp_code가 있는 애들만)
    comps = sb.table("companies").select("company_key, dart_corp_code").execute().data or []
    for c in comps:
        if not c.get("dart_corp_code"):
            continue
        ck = c["company_key"]
        corp_code = c["dart_corp_code"]
        dart_payload = fetch_fnltt_singl_acnt_all(corp_code, bsns_year=str(today.year-1), reprt_code="11011")
        insert_raw_event(sb, "DART", "fnlttSinglAcntAll", dart_payload["params"], dart_payload["data"], as_of=today)
        normalize_fnltt(sb, ck, dart_payload, bsns_year=str(today.year-1), reprt_code="11011")

    # 3) KOSIS ingest (예시: 네가 params를 지정해야 함)
    # params 예시는 너의 KOSIS 사용 스펙에 맞춰 바꿔야 함
    # kosis_payload = fetch_kosis({"method": "...", "itmId": "...", "objL1": "...", "format": "json"})
    # insert_raw_event(sb, "KOSIS", "market", kosis_payload["params"], {"data": kosis_payload["data"]}, as_of=today)
    # normalize_kosis_market(sb, kosis_payload, indicator_key="KOSIS:market_size:KR:example", year=today.year)

    # 4) build features → train → predict
    run_build_features(sb, as_of=today)
    ver = run_train(sb, as_of=today)
    run_predict(sb, model_path="tmp/model.pkl", as_of=today, model_version=ver)

    print("Pipeline complete.")

if __name__ == "__main__":
    main()

