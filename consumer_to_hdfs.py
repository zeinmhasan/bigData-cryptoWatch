import json
import os
import threading
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from hdfs import InsecureClient

# ─── Konfigurasi ─────────────────────────────────────────────────
KAFKA_BROKER   = "kafka:29092"              # ← service name Docker
HDFS_URL       = "http://namenode:9870"     # ← WebHDFS namenode
FLUSH_INTERVAL = 120                        # flush setiap 2 menit
DASHBOARD_DIR  = "/app/dashboard/data"      # ← path di dalam container

HDFS_API_PATH  = "/data/crypto/api"
HDFS_RSS_PATH  = "/data/crypto/rss"

os.makedirs(DASHBOARD_DIR, exist_ok=True)

# ─── HDFS Client ─────────────────────────────────────────────────
def create_hdfs_client():
    while True:
        try:
            client = InsecureClient(HDFS_URL, user="root")
            client.status("/")  # test koneksi
            print(f" Terhubung ke HDFS: {HDFS_URL}")
            return client
        except Exception as e:
            print(f" HDFS belum siap, retry 5 detik... ({e})")
            time.sleep(5)

hdfs_client = create_hdfs_client()

# ─── Pastikan direktori HDFS ada ─────────────────────────────────
for path in [HDFS_API_PATH, HDFS_RSS_PATH, "/data/crypto/hasil"]:
    try:
        hdfs_client.makedirs(path)
    except Exception:
        pass  # sudah ada

# ─── Buffer ──────────────────────────────────────────────────────
buffers = {"crypto-api": [], "crypto-rss": []}
locks   = {"crypto-api": threading.Lock(), "crypto-rss": threading.Lock()}

# ─── Tunggu Kafka siap ───────────────────────────────────────────
def create_consumer(topic):
    while True:
        try:
            c = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                group_id=f"hdfs-consumer-{topic}",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print(f" Consumer terhubung ke topic: {topic}")
            return c
        except Exception as e:
            print(f" Kafka belum siap untuk topic {topic}, retry 5 detik... ({e})")
            time.sleep(5)

# ─── Simpan ke HDFS via Python client ────────────────────────────
def save_to_hdfs(topic, data):
    if not data:
        return
    ts       = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{topic}_{ts}.json"
    hdfs_dir = HDFS_API_PATH if topic == "crypto-api" else HDFS_RSS_PATH
    hdfs_path = f"{hdfs_dir}/{filename}"

    try:
        content = json.dumps(data).encode("utf-8")
        with hdfs_client.write(hdfs_path, overwrite=True) as writer:
            writer.write(content)
        print(f"   HDFS [{topic}] → {filename} ({len(data)} events)")
    except Exception as e:
        print(f"  [ERROR] Gagal upload HDFS: {e}")

# ─── Update file JSON untuk dashboard ────────────────────────────
def save_live_dashboard(topic, data):
    if not data:
        return

    if topic == "crypto-api":
        path = os.path.join(DASHBOARD_DIR, "live_api.json")
        latest = {}
        for event in data:
            latest[event.get("symbol", "")] = event
        with open(path, "w") as f:
            json.dump(list(latest.values()), f, indent=2)
        print(f"   live_api.json diupdate ({len(latest)} koin)")

    elif topic == "crypto-rss":
        path = os.path.join(DASHBOARD_DIR, "live_rss.json")
        existing = []
        if os.path.exists(path):
            try:
                with open(path) as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        combined = data + existing
        seen, unique = set(), []
        for item in combined:
            aid = item.get("article_id", "")
            if aid not in seen:
                seen.add(aid)
                unique.append(item)

        with open(path, "w") as f:
            json.dump(unique[:20], f, indent=2)
        print(f"   live_rss.json diupdate ({len(unique[:20])} artikel)")

# ─── Flush periodik ──────────────────────────────────────────────
def flush_loop():
    while True:
        time.sleep(FLUSH_INTERVAL)
        for topic in ["crypto-api", "crypto-rss"]:
            with locks[topic]:
                data = buffers[topic].copy()
                buffers[topic].clear()
            if data:
                print(f"\n Flushing {len(data)} events dari {topic}...")
                save_to_hdfs(topic, data)
                save_live_dashboard(topic, data)

# ─── Consumer thread ─────────────────────────────────────────────
def consume_topic(topic):
    consumer = create_consumer(topic)
    print(f"   Listening topic: {topic}")
    for msg in consumer:
        with locks[topic]:
            buffers[topic].append(msg.value)

# ─── Main ────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(" Consumer HDFS started")
    print(f"   Kafka broker   : {KAFKA_BROKER}")
    print(f"   HDFS URL       : {HDFS_URL}")
    print(f"   Dashboard dir  : {DASHBOARD_DIR}")
    print(f"   Flush interval : {FLUSH_INTERVAL} detik\n")

    t1 = threading.Thread(target=consume_topic, args=("crypto-api",), daemon=True)
    t2 = threading.Thread(target=consume_topic, args=("crypto-rss",), daemon=True)
    t1.start()
    t2.start()

    flush_loop()