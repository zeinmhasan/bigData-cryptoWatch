import json
import time
import hashlib
import feedparser
from datetime import datetime, timezone
from kafka import KafkaProducer

# ─── Konfigurasi ─────────────────────────────────────────────────
KAFKA_BROKER = "kafka:29092"          # ← pakai service name Docker
TOPIC        = "crypto-rss"
INTERVAL     = 300  # 5 menit

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss"
]

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
            print(f" Terhubung ke Kafka: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f" Kafka belum siap, retry 5 detik... ({e})")
            time.sleep(5)

producer = create_producer()
sent_ids = set()

def make_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:8]

def fetch_and_send():
    total_sent = 0
    ts = datetime.now(timezone.utc).isoformat()

    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.bozo:
                print(f"[WARN] Feed bermasalah: {feed_url}")
                continue

            for entry in feed.entries:
                url        = getattr(entry, "link", "")
                article_id = make_id(url)
                if article_id in sent_ids:
                    continue

                event = {
                    "article_id": article_id,
                    "title":      getattr(entry, "title",     "No Title"),
                    "link":       url,
                    "summary":    getattr(entry, "summary",   "")[:500],
                    "published":  getattr(entry, "published", ts),
                    "source":     feed_url,
                    "timestamp":  ts
                }
                producer.send(TOPIC, key=article_id, value=event)
                sent_ids.add(article_id)
                total_sent += 1
                print(f"   [{article_id}] {event['title'][:60]}...")

            producer.flush()

        except Exception as e:
            print(f"[ERROR] Gagal fetch {feed_url}: {e}")

    print(f"[{ts}] Total terkirim: {total_sent} artikel baru\n")

if __name__ == "__main__":
    print(f" Producer RSS started — polling setiap {INTERVAL//60} menit")
    while True:
        print(" Fetching RSS feeds...")
        fetch_and_send()
        print(f" Menunggu {INTERVAL//60} menit...\n")
        time.sleep(INTERVAL)