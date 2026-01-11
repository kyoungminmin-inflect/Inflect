from datetime import datetime, timezone

from src.db.supabase_client import get_supabase, insert_raw_event
from src.ingest.ingest_census import fetch_abscs_us_level
from src.ingest.ingest_kosis import fetch_kosis_example
from src.ml.build_features import build_hourly_features
from src.ml.train import compute_scores

def main():
    sb = get_supabase()
    run_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # 1) ingest (raw 저장)
    census_payload = fetch_abscs_us_level()
    insert_raw_event(sb, source="census", topic="abscs_us_level", payload=census_payload, asset="US")

    kosis_payload = fetch_kosis_example()
    insert_raw_event(sb, source="kosis", topic="kosis_example", payload=kosis_payload, asset="KR")

    # 2) features
    build_hourly_features(sb, run_at)

    # 3) scores
    compute_scores(sb, run_at)

    print("OK - hourly pipeline finished:", run_at.isoformat())

if __name__ == "__main__":
    main()
