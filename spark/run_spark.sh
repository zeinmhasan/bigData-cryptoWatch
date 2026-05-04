#!/bin/bash
echo "Install pandas..."
pip3 install pandas -q

echo "=============================="
echo "Run forecast awal (pakai data HDFS yang sudah ada)..."
/opt/spark/bin/spark-submit --master local[*] /opt/spark/work/ml_forecast.py 2>/dev/null \
    && echo "Forecast awal selesai" \
    || echo "Forecast awal gagal (HDFS mungkin kosong), akan dicoba lagi nanti"

echo "=============================="
echo "Run analysis awal..."
/opt/spark/bin/spark-submit --master local[*] /opt/spark/work/analysis.py 2>/dev/null \
    && echo "Analysis awal selesai" \
    || echo "Analysis awal gagal, akan dicoba lagi nanti"

echo "=============================="
echo "Tunggu 3 menit untuk data baru masuk dari consumer..."
sleep 180

while true; do
    echo "=============================="
    echo "Memulai Spark Analysis..."
    /opt/spark/bin/spark-submit --master local[*] /opt/spark/work/analysis.py 2>/dev/null \
        && echo "Analysis selesai sukses" \
        || echo "Analysis gagal, coba lagi nanti"

    echo "Memulai SparkML Forecast..."
    /opt/spark/bin/spark-submit --master local[*] /opt/spark/work/ml_forecast.py 2>/dev/null \
        && echo "Forecast selesai sukses" \
        || echo "Forecast gagal, coba lagi nanti"

    echo "Menunggu 3 menit..."
    sleep 180
done
