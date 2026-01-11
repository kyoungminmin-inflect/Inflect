import requests
from datetime import datetime

def main():
    print("=== Inflect Market Agent ===")

    # 1. 비트코인 가격 가져오기 (연습용)
    url = "https://api.coindesk.com/v1/bpi/currentprice/BTC.json"
    data = requests.get(url).json()

    price_usd = data["bpi"]["USD"]["rate_float"]
    updated_time = data["time"]["updated"]

    # 2. 간단한 판단
    signal = "HOLD"
    if price_usd > 50000:
        signal = "SELL"
    elif price_usd < 30000:
        signal = "BUY"

    # 3. 결과 출력
    print(f"Time: {updated_time}")
    print(f"BTC Price (USD): {price_usd}")
    print(f"Signal: {signal}")

if __name__ == "__main__":
    main()
