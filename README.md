# CryptoWatch

## Kelompok 2

- Zein Muhammad Hasan (5027241035)
- Muhammad Ahsani Taqwiim (5027241099)
- Andi Naufal Zaki (5027241059)
- Aslam Ahmad Usman (5027241074)
- Naila Cahyarani Idelia (5027241063)

## Ringkasan

CryptoWatch adalah pipeline data crypto end-to-end yang mengumpulkan harga koin dan berita, mengalirkannya lewat Kafka, menyimpan ke HDFS, melakukan analisis dan forecasting dengan Spark, lalu menampilkan hasilnya di dashboard web (Flask).

## Fitur Utama

1. Ingest harga crypto dari API
   - Sumber utama: CoinGecko
   - Fallback otomatis: CryptoCompare
   - Interval polling 60 detik
   - Koin: BTC, ETH, BNB

2. Ingest berita crypto dari RSS
   - Sumber: CoinDesk, CoinTelegraph
   - Interval polling 5 menit
   - Dedup berdasarkan hash URL

3. Streaming dan penyimpanan
   - Kafka topic: crypto-api dan crypto-rss
   - Consumer menulis batch ke HDFS setiap 2 menit
   - Menulis file JSON ringkas untuk dashboard

4. Analisis Spark (batch)
   - Statistik harga per koin (rata-rata, min, max, std dev, jumlah data)
   - Volatilitas per jam
   - Volume berita per jam
   - Output disimpan ke HDFS dan juga JSON untuk dashboard

5. Forecasting SparkML
   - Target: persentase perubahan harga
   - Model kandidat: Linear Regression, Random Forest, GBT
   - Memilih model terbaik berdasarkan R2
   - Hasil forecast 10 langkah ke depan

6. Dashboard Web
   - Harga live, analisis Spark, berita terbaru, grafik forecast
   - Endpoint JSON untuk konsumsi frontend

7. Monitoring dan diagnosis
   - Script diagnosa untuk cek container, Kafka, HDFS, dan freshness data

## Arsitektur Singkat

- Producer API -> Kafka (crypto-api)
- Producer RSS -> Kafka (crypto-rss)
- Consumer -> HDFS + file JSON dashboard
- Spark Analysis + SparkML -> JSON dashboard
- Flask Dashboard -> membaca JSON dan menampilkan grafik

## Struktur Folder

- consumer_to_hdfs.py
- producer_api.py
- producer_rss.py
- dashboard/
  - app.py
  - templates/index.html
  - data/\*.json
- spark/
  - analysis.py
  - ml_forecast.py
  - run_spark.sh
- docker-compose.yml
- Dockerfile
- requirements.txt
- diagnose.sh

## Cara Menjalankan (Docker Compose)

### Prasyarat

- Docker dan Docker Compose terpasang
- Koneksi internet (akses ke API dan RSS)

### Langkah

1. Build dan jalankan semua service:
   docker compose up -d --build

2. Cek status container:
   docker ps

3. Akses dashboard:
   http://localhost:5000

4. HDFS UI (optional):
   http://localhost:9870

5. Hentikan semua service:
   docker compose down

## Cara Pakai (Local tanpa Docker)

Disarankan tetap menggunakan Docker karena butuh Kafka, HDFS, dan Spark. Jika ingin manual:

1. Jalankan Kafka dan HDFS secara terpisah
2. Install dependencies:
   pip install -r requirements.txt
3. Jalankan producer dan consumer:
   python producer_api.py
   python producer_rss.py
   python consumer_to_hdfs.py
4. Jalankan Spark Analysis dan Forecast:
   spark-submit spark/analysis.py
   spark-submit spark/ml_forecast.py
5. Jalankan dashboard:
   python dashboard/app.py

## Endpoint Dashboard

- GET /api/data
  - Mengembalikan gabungan data live, hasil spark, dan berita
- GET /api/forecast
  - Mengembalikan hasil forecast per koin

## Output Data

- dashboard/data/live_api.json
- dashboard/data/live_rss.json
- dashboard/data/spark_results.json
- dashboard/data/forecast.json

## Diagnostik dan Troubleshooting

Jalankan script berikut untuk cek semua komponen:

- bash diagnose.sh

Yang dicek:

- Status container
- Koneksi internet di container
- Umur data JSON dashboard
- Error log container
- Jumlah pesan di Kafka
- Ketersediaan file di HDFS

## Tampilan Monitor
<img width="1920" height="3682" alt="screencapture-localhost-5000-2026-05-04-14_18_36" src="https://github.com/user-attachments/assets/57881e67-5967-4852-b7cc-6a07ddf78aac" />


## Catatan

- Jika data tidak update, cek container consumer-hdfs dan koneksi API.
- Spark dijalankan periodik oleh script spark/run_spark.sh di container spark-analyzer.
