import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Init SparkSession ───────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoWatch Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_API  = "hdfs://namenode:8020/data/crypto/api/"
HDFS_RSS  = "hdfs://namenode:8020/data/crypto/rss/"
OUTPUT_DIR = "/opt/dashboard-data"

print("=" * 55)
print("  CryptoWatch — Spark Analysis")
print("=" * 55)

# ─── Load Data ───────────────────────────────────────────────────
print("\n📂 Membaca data dari HDFS...")

df_api = spark.read.option("multiLine", True).json(HDFS_API)
df_rss = spark.read.option("multiLine", True).json(HDFS_RSS)

# Flatten jika data berbentuk array
if "symbol" not in df_api.columns:
    df_api = df_api.select(F.explode(F.col(df_api.columns[0])).alias("data")).select("data.*")

if "article_id" not in df_rss.columns:
    df_rss = df_rss.select(F.explode(F.col(df_rss.columns[0])).alias("data")).select("data.*")

print(f"  ✅ API records : {df_api.count()}")
print(f"  ✅ RSS records : {df_rss.count()}")

df_api.createOrReplaceTempView("crypto_api")
df_rss.createOrReplaceTempView("crypto_rss")

# ════════════════════════════════════════════════════════
# ANALISIS 1 — Statistik Harga per Koin
# ════════════════════════════════════════════════════════
print("\n" + "─" * 55)
print("📊 ANALISIS 1: Statistik Harga per Koin")
print("─" * 55)

df_stats = spark.sql("""
    SELECT
        symbol,
        ROUND(AVG(price_usd), 2)    AS rata_rata_usd,
        ROUND(MAX(price_usd), 2)    AS tertinggi_usd,
        ROUND(MIN(price_usd), 2)    AS terendah_usd,
        ROUND(STDDEV(price_usd), 2) AS std_deviasi,
        COUNT(*)                    AS jumlah_data
    FROM crypto_api
    GROUP BY symbol
    ORDER BY rata_rata_usd DESC
""")
df_stats.show(truncate=False)

# ════════════════════════════════════════════════════════
# ANALISIS 2 — Volatilitas per Jam
# ════════════════════════════════════════════════════════
print("\n" + "─" * 55)
print("📊 ANALISIS 2: Volatilitas per Jam")
print("─" * 55)

df_volatility = spark.sql("""
    SELECT
        HOUR(TO_TIMESTAMP(timestamp)) AS jam,
        symbol,
        ROUND(AVG(ABS(change_24h)), 4) AS avg_volatilitas,
        COUNT(*) AS jumlah_data
    FROM crypto_api
    GROUP BY jam, symbol
    ORDER BY avg_volatilitas DESC
""")
df_volatility.show(20, truncate=False)

# ════════════════════════════════════════════════════════
# ANALISIS 3 — Volume Berita per Jam
# ════════════════════════════════════════════════════════
print("\n" + "─" * 55)
print("📊 ANALISIS 3: Volume Berita per Jam")
print("─" * 55)

df_news = spark.sql("""
    SELECT
        HOUR(TO_TIMESTAMP(timestamp)) AS jam,
        COUNT(*) AS jumlah_artikel
    FROM crypto_rss
    GROUP BY jam
    ORDER BY jumlah_artikel DESC
""")
df_news.show(truncate=False)

# ─── Simpan hasil ke HDFS ────────────────────────────────
print("\n💾 Menyimpan hasil ke HDFS...")
df_stats.write.mode("overwrite").json("hdfs://namenode:8020/data/crypto/hasil/statistik")
df_volatility.write.mode("overwrite").json("hdfs://namenode:8020/data/crypto/hasil/volatilitas")
df_news.write.mode("overwrite").json("hdfs://namenode:8020/data/crypto/hasil/berita")
print("  ✅ Tersimpan di /data/crypto/hasil/")

# ─── Simpan ke JSON lokal untuk dashboard ────────────────
print("\n📤 Menyimpan spark_results.json untuk dashboard...")
results = {
    "statistik":   df_stats.toPandas().to_dict(orient="records"),
    "volatilitas": df_volatility.toPandas().to_dict(orient="records"),
    "berita":      df_news.toPandas().to_dict(orient="records")
}

out_path = os.path.join(OUTPUT_DIR, "spark_results.json")
with open(out_path, "w") as f:
    json.dump(results, f, indent=2)

print(f"  ✅ Tersimpan di {out_path}")
print("\n✅ Analisis selesai!")
spark.stop()
