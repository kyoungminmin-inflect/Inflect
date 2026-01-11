import os
import time
import requests
from supabase import create_client, Client


def fetch_json(url: str, retries: int = 3, timeout: int = 20) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (i + 1))  # 2, 4, 6초 대기
    raise last_err


def main():
    print("=== Inflect Market Agent ===")

    # 1) 시장 데이터 수집 (CoinGecko)
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    data = fetch_json(url)

    price_usd = float(data["bitcoin"]["usd"])
    symbol = "BTC"
    asset = "bitcoin"
    source = "coingecko"

    # 2) 간단 시그널 (예시 룰)
    signal = "HOLD"
    if price_usd > 50000:
        signal = "SELL"
    elif price_usd < 30000:
        signal = "BUY"

    print(f"BTC Price (USD): {price_usd}")
    print(f"Signal: {signal}")

    # 3) Supabase 연결 (Actions secrets로 주입)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("Supabase env not set. Skipping DB insert.")
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    # 4) DB insert (테이블 스키마와 1:1 매칭)
    row = {
        "symbol": symbol,
        "asset": asset,
        "source": source,
        "price_usd": price_usd,
        "signal": signal,
        "raw": data,
    }

    # 진단 로그(문제 생기면 여기로 확인)
    print("DEBUG row:", row)

    res = supabase.table("market_runs").insert(row).execute()
    print("Inserted to Supabase:", res.data)


if __name__ == "__main__":
    main()
