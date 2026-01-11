import time
import requests

def fetch_json(url: str, retries: int = 3, timeout: int = 20) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (i + 1))  # 2초, 4초, 6초 대기
    raise last_err

def main():
    print("=== Inflect Market Agent ===")

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    data = fetch_json(url)

    price_usd = data["bitcoin"]["usd"]

    signal = "HOLD"
    if price_usd > 50000:
        signal = "SELL"
    elif price_usd < 30000:
        signal = "BUY"

    print(f"BTC Price (USD): {price_usd}")
    print(f"Signal: {signal}")

if __name__ == "__main__":
    main()
