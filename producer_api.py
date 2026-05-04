import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

# ─── Konfigurasi ─────────────────────────────────────────────────
KAFKA_BROKER = "kafka:29092"
TOPIC        = "crypto-api"
INTERVAL     = 60

USD_TO_IDR   = 16500  # fallback kurs manual

# ─── Tunggu Kafka siap ───────────────────────────────────────────
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                enable_idempotence=True,
                acks="all",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            print(f"✅ Terhubung ke Kafka: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"⏳ Kafka belum siap, retry 5 detik... ({e})")
            time.sleep(5)

producer = create_producer()

# ─── Sumber 1: CoinGecko ─────────────────────────────────────────
def fetch_coingecko():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,binancecoin",
        "vs_currencies": "usd,idr",
        "include_24hr_change": "true"
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    mapping = {"bitcoin": "BTC", "ethereum": "ETH", "binancecoin": "BNB"}
    result = []
    for coin_id, symbol in mapping.items():
        result.append({
            "symbol":     symbol,
            "coin_id":    coin_id,
            "price_usd":  round(data[coin_id]["usd"], 2),
            "price_idr":  round(data[coin_id]["idr"], 0),
            "change_24h": round(data[coin_id].get("usd_24h_change", 0), 4),
        })
    return result, "CoinGecko"

# ─── Sumber 2: CryptoCompare (fallback) ──────────────────────────
def fetch_cryptocompare():
    url = "https://min-api.cryptocompare.com/data/pricemultifull"
    params = {"fsyms": "BTC,ETH,BNB", "tsyms": "USD"}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    raw = resp.json()["RAW"]

    mapping = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin"}
    result = []
    for symbol, coin_id in mapping.items():
        d = raw[symbol]["USD"]
        price_usd = round(d["PRICE"], 2)
        result.append({
            "symbol":     symbol,
            "coin_id":    coin_id,
            "price_usd":  price_usd,
            "price_idr":  round(price_usd * USD_TO_IDR, 0),
            "change_24h": round(d.get("CHANGEPCT24HOUR", 0), 4),
        })
    return result, "CryptoCompare"

# ─── Fetch dengan fallback otomatis ──────────────────────────────
def fetch_with_fallback():
    for fetch_fn in [fetch_coingecko, fetch_cryptocompare]:
        try:
            data, source = fetch_fn()
            print(f"   📡 Data dari: {source}")
            return data
        except Exception as e:
            print(f"   ⚠ {fetch_fn.__name__} gagal: {str(e)[:80]}")
            continue
    print("[ERROR] Semua sumber API gagal!")
    return None

# ─── Main ─────────────────────────────────────────────────────────
def fetch_and_send():
    ts   = datetime.now(timezone.utc).isoformat()
    data = fetch_with_fallback()

    if not data:
        return

    for coin in data:
        event = {**coin, "timestamp": ts}
        producer.send(TOPIC, key=coin["symbol"], value=event)
        print(f"[{ts}] Sent {coin['symbol']}: ${coin['price_usd']:,.2f} | {coin['change_24h']:+.2f}%")

    producer.flush()

if __name__ == "__main__":
    print(f"🚀 Producer API started — polling setiap {INTERVAL} detik")
    print(f"   Broker : {KAFKA_BROKER}")
    print(f"   Fallback: CoinGecko → CryptoCompare\n")
    while True:
        fetch_and_send()
        print(f"   ⏳ Menunggu {INTERVAL} detik...\n")
        time.sleep(INTERVAL)
