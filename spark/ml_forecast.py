import json
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("CryptoWatch ML Forecast v3") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HDFS_API   = "hdfs://namenode:8020/data/crypto/api/"
OUTPUT_DIR = "/opt/dashboard-data"
HORIZON    = 10

print("=" * 60)
print("  CryptoWatch — SparkML Forecast v3 (Price Change Target)")
print("=" * 60)

# ─── Load ────────────────────────────────────────────────────
print("\n📂 Membaca data dari HDFS...")
df_raw = spark.read.option("multiLine", True).json(HDFS_API)

if "symbol" not in df_raw.columns:
    df_raw = df_raw.select(F.explode(F.col(df_raw.columns[0])).alias("d")).select("d.*")

df_base = df_raw.select(
    F.col("symbol"),
    F.col("price_usd").cast("double").alias("price"),
    F.unix_timestamp(F.to_timestamp(F.col("timestamp"))).cast("double").alias("ts")
).dropna().orderBy("symbol", "ts")

total = df_base.count()
print(f"  ✅ Total records: {total}")

if total < 20:
    print("  ⚠ Data terlalu sedikit (minimal 20), skip")
    spark.stop()
    exit(0)

COINS   = ["BTC", "ETH", "BNB"]
results = {}

for coin in COINS:
    print(f"\n{'='*60}")
    print(f"🔮 Forecasting {coin}...")

    df_coin = df_base.filter(F.col("symbol") == coin).orderBy("ts")
    count   = df_coin.count()

    if count < 20:
        print(f"  ⚠ Data tidak cukup ({count}), skip")
        continue

    # ── Feature Engineering ───────────────────────────────────
    # TARGET: pct_change = persentase perubahan harga
    # FITUR : indikator teknikal yang tidak bocorkan target
    w = Window.orderBy("ts")

    df_feat = df_coin \
        .withColumn("pct_change",
            (F.col("price") - F.lag("price", 1).over(w)) /
             F.lag("price", 1).over(w) * 100) \
        .withColumn("pct_lag1", F.lag("pct_change", 1).over(w)) \
        .withColumn("pct_lag2", F.lag("pct_change", 2).over(w)) \
        .withColumn("pct_lag3", F.lag("pct_change", 3).over(w)) \
        .withColumn("ma7",  F.avg("price").over(w.rowsBetween(-6,  0))) \
        .withColumn("ma14", F.avg("price").over(w.rowsBetween(-13, 0))) \
        .withColumn("ma_ratio",   F.col("price") / F.col("ma7"))  \
        .withColumn("ma_cross",   F.col("ma7")   / F.col("ma14")) \
        .withColumn("volatility",
            F.stddev("pct_change").over(w.rowsBetween(-6, 0))) \
        .withColumn("momentum",
            (F.col("price") - F.lag("price", 5).over(w)) /
             F.lag("price", 5).over(w) * 100) \
        .dropna()

    feat_cols = ["pct_lag1", "pct_lag2", "pct_lag3",
                 "ma_ratio", "ma_cross", "volatility", "momentum"]

    assembler = VectorAssembler(inputCols=feat_cols, outputCol="raw_features")
    scaler    = StandardScaler(inputCol="raw_features", outputCol="features",
                               withMean=True, withStd=True)

    # ── Train/Val split 80/20 ─────────────────────────────────
    all_rows = df_feat.orderBy("ts").collect()
    n_train  = int(len(all_rows) * 0.8)
    train_rows = all_rows[:n_train]
    val_rows   = all_rows[n_train:]

    if len(val_rows) < 5:
        print(f"  ⚠ Val set terlalu kecil, skip")
        continue

    df_train = spark.createDataFrame(train_rows, df_feat.schema)
    df_val   = spark.createDataFrame(val_rows,   df_feat.schema)

    print(f"  📊 Train: {len(train_rows)} | Val: {len(val_rows)}")
    print(f"  🎯 Target: pct_change (% perubahan harga per interval)")

    evaluator = RegressionEvaluator(
        labelCol="pct_change", predictionCol="prediction", metricName="r2")
    mae_eval  = RegressionEvaluator(
        labelCol="pct_change", predictionCol="prediction", metricName="mae")

    # ── 3 Model ───────────────────────────────────────────────
    models_def = {
        "LinearRegression": Pipeline(stages=[
            assembler, scaler,
            LinearRegression(featuresCol="features", labelCol="pct_change", maxIter=200)
        ]),
        "RandomForest": Pipeline(stages=[
            assembler, scaler,
            RandomForestRegressor(featuresCol="features", labelCol="pct_change",
                                  numTrees=100, maxDepth=6, seed=42)
        ]),
        "GBT": Pipeline(stages=[
            assembler, scaler,
            GBTRegressor(featuresCol="features", labelCol="pct_change",
                         maxIter=80, maxDepth=5, seed=42)
        ]),
    }

    best_model      = None
    best_model_name = ""
    best_r2         = -999
    best_mae        = 0

    print(f"  🏁 Evaluasi model (target: % perubahan harga):")
    for name, pipeline in models_def.items():
        try:
            fitted   = pipeline.fit(df_train)
            val_pred = fitted.transform(df_val)
            r2  = round(evaluator.evaluate(val_pred), 4)
            mae = round(mae_eval.evaluate(val_pred), 6)
            print(f"     {name:20s} → R²={r2:.4f} | MAE={mae:.6f}%")

            if r2 > best_r2:
                best_r2         = r2
                best_mae        = mae
                best_model      = fitted
                best_model_name = name
        except Exception as e:
            print(f"     {name:20s} → GAGAL: {e}")

    if best_model is None:
        print(f"  ✘ Semua model gagal untuk {coin}")
        continue

    print(f"  🏆 Model terbaik: {best_model_name} (R²={best_r2})")

    # ── Retrain dengan semua data ─────────────────────────────
    final_model = models_def[best_model_name].fit(df_feat)

    # ── History ───────────────────────────────────────────────
    history_rows = all_rows[-50:]
    history = [
        {"timestamp": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
         "price":     round(r["price"], 2)}
        for r in history_rows
    ]

    # ── Estimasi interval ─────────────────────────────────────
    interval = 60
    if len(all_rows) > 1:
        diffs = [all_rows[i]["ts"] - all_rows[i-1]["ts"]
                 for i in range(max(1, len(all_rows)-10), len(all_rows))]
        interval = round(sum(diffs) / len(diffs))

    # ── Prediksi iteratif (pakai % change) ───────────────────
    ctx = list(all_rows[-15:])  # sliding context window
    last_ts = all_rows[-1]["ts"]

    forecast = []
    for i in range(1, HORIZON + 1):
        future_ts = last_ts + (interval * i)

        prices  = [r["price"] for r in ctx]
        pcts    = [r["pct_change"] for r in ctx]

        pct_lag1   = pcts[-1]
        pct_lag2   = pcts[-2] if len(pcts) >= 2 else pct_lag1
        pct_lag3   = pcts[-3] if len(pcts) >= 3 else pct_lag2
        ma7        = sum(prices[-7:])  / min(7,  len(prices))
        ma14       = sum(prices[-14:]) / min(14, len(prices))
        ma_ratio   = prices[-1] / ma7 if ma7 else 1.0
        ma_cross   = ma7 / ma14 if ma14 else 1.0
        mean_pct   = sum(pcts[-7:]) / min(7, len(pcts))
        vol        = (sum((p - mean_pct)**2 for p in pcts[-7:]) / min(7, len(pcts))) ** 0.5
        momentum   = (prices[-1] - prices[-5]) / prices[-5] * 100 if len(prices) >= 5 else 0.0

        row_schema = {
            "symbol": coin, "price": prices[-1], "ts": future_ts,
            "pct_change": 0.0,
            "pct_lag1": pct_lag1, "pct_lag2": pct_lag2, "pct_lag3": pct_lag3,
            "ma7": ma7, "ma14": ma14,
            "ma_ratio": ma_ratio, "ma_cross": ma_cross,
            "volatility": vol, "momentum": momentum
        }

        df_pred    = spark.createDataFrame([row_schema], df_feat.schema)
        pred_pct   = final_model.transform(df_pred).select("prediction").collect()[0][0]

        # Konversi % change → harga absolut
        prev_price = prices[-1]
        pred_price = round(prev_price * (1 + pred_pct / 100), 2)

        forecast.append({
            "timestamp": datetime.fromtimestamp(future_ts, tz=timezone.utc).isoformat(),
            "price":     pred_price
        })

        # Update context
        ctx.append({
            "price": pred_price, "ts": future_ts, "pct_change": pred_pct,
            "pct_lag1": pct_lag1, "pct_lag2": pct_lag2, "pct_lag3": pct_lag3,
            "ma7": ma7, "ma14": ma14, "ma_ratio": ma_ratio, "ma_cross": ma_cross,
            "volatility": vol, "momentum": momentum, "symbol": coin
        })

    results[coin] = {
        "history":  history,
        "forecast": forecast,
        "model": {
            "name": best_model_name,
            "r2":   best_r2,
            "mae":  round(best_mae, 6)
        }
    }

    print(f"  ✅ History: {len(history)} | Forecast: {len(forecast)}")
    print(f"  💰 Harga terakhir : ${history[-1]['price']:,.2f}")
    print(f"  🔮 Prediksi akhir : ${forecast[-1]['price']:,.2f}")

# ─── Simpan ──────────────────────────────────────────────────
out_path = os.path.join(OUTPUT_DIR, "forecast.json")
with open(out_path, "w") as f:
    json.dump(results, f, indent=2)

print(f"\n✅ Forecast v3 disimpan ke {out_path}")
spark.stop()
