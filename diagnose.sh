#!/bin/bash
# ═══════════════════════════════════════════════════════
#  CryptoWatch — Full Diagnostic Script
# ═══════════════════════════════════════════════════════

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "  ${GREEN}✔${RESET}  $1"; }
fail() { echo -e "  ${RED}✘${RESET}  $1"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $1"; }
info() { echo -e "  ${CYAN}ℹ${RESET}  $1"; }

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║       CryptoWatch — Full Diagnostic                  ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${RESET}"
echo -e "  Waktu: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# ─────────────────────────────────────────────
# 1. CONTAINER STATUS
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 1 ] STATUS CONTAINER${RESET}"
echo "──────────────────────────────────────────────────────"
ALL_OK=true
for name in zookeeper kafka namenode datanode producer-api producer-rss consumer-hdfs spark-analyzer dashboard; do
    status=$(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null)
    restarts=$(docker inspect --format='{{.RestartCount}}' "$name" 2>/dev/null)
    if [ "$status" = "running" ]; then
        if [ "$restarts" -gt 3 ] 2>/dev/null; then
            warn "$name → running (restart: ${RED}${restarts}x${RESET} — ada masalah!)"
            ALL_OK=false
        else
            ok "$name → running (restart: ${restarts}x)"
        fi
    else
        fail "$name → ${status:-not found}"
        ALL_OK=false
    fi
done
echo ""

# ─────────────────────────────────────────────
# 2. KONEKSI INTERNET DARI CONTAINER
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 2 ] KONEKSI INTERNET (dari container producer-api)${RESET}"
echo "──────────────────────────────────────────────────────"

# DNS resolve
dns_result=$(docker exec producer-api python -c "import socket; print(socket.gethostbyname('api.coingecko.com'))" 2>&1)
if echo "$dns_result" | grep -qE "^[0-9]+\.[0-9]+"; then
    ok "DNS resolve api.coingecko.com → $dns_result"
else
    fail "DNS resolve GAGAL → $dns_result"
fi

# Test tiap API endpoint
declare -A APIS
APIS["CoinGecko"]="https://api.coingecko.com/api/v3/ping"
APIS["Binance"]="https://api.binance.com/api/v3/ping"
APIS["CryptoCompare"]="https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
APIS["Coinbase"]="https://api.coinbase.com/v2/prices/BTC-USD/spot"
APIS["Kraken"]="https://api.kraken.com/0/public/Ticker?pair=XBTUSD"

for api_name in "${!APIS[@]}"; do
    url="${APIS[$api_name]}"
    result=$(docker exec producer-api python -c "
import requests, sys
try:
    r = requests.get('$url', timeout=8)
    print('OK:' + str(r.status_code))
except Exception as e:
    print('FAIL:' + str(e)[:80])
" 2>/dev/null)
    if echo "$result" | grep -q "^OK:2"; then
        ok "$api_name → ${GREEN}bisa diakses${RESET} ($result)"
    else
        fail "$api_name → ${RED}tidak bisa${RESET} ($result)"
    fi
done
echo ""

# ─────────────────────────────────────────────
# 3. CEK DATA JSON - KAPAN TERAKHIR UPDATE
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 3 ] FRESHNESS DATA JSON${RESET}"
echo "──────────────────────────────────────────────────────"
DASH_DIR="$HOME/cryptowatch/dashboard/data"
NOW=$(date +%s)

for file in live_api.json live_rss.json spark_results.json; do
    fpath="$DASH_DIR/$file"
    if [ -f "$fpath" ]; then
        mod_time=$(stat -c %Y "$fpath" 2>/dev/null)
        age=$(( (NOW - mod_time) / 60 ))
        mod_str=$(stat -c '%y' "$fpath" | cut -d'.' -f1)

        if [ "$age" -lt 5 ]; then
            ok "$file → update ${age} menit lalu ($mod_str)"
        elif [ "$age" -lt 30 ]; then
            warn "$file → update ${age} menit lalu ($mod_str) — agak lama"
        else
            fail "$file → update ${age} menit lalu ($mod_str) — STALE! data tidak update"
        fi
    else
        fail "$file → FILE TIDAK ADA"
    fi
done
echo ""

# ─────────────────────────────────────────────
# 4. CEK LOG ERROR TIAP CONTAINER
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 4 ] ERROR DI LOG CONTAINER${RESET}"
echo "──────────────────────────────────────────────────────"

check_log() {
    local name=$1
    local errors=$(docker logs "$name" --tail=50 2>&1 | grep -i "error\|failed\|exception\|traceback" | tail -3)
    if [ -n "$errors" ]; then
        warn "$name ada error:"
        echo "$errors" | while read line; do
            echo -e "       ${RED}$line${RESET}"
        done
    else
        ok "$name → tidak ada error di 50 log terakhir"
    fi
}

for name in producer-api producer-rss consumer-hdfs spark-analyzer dashboard; do
    check_log "$name"
done
echo ""

# ─────────────────────────────────────────────
# 5. CEK KAFKA - ADA DATA MASUK?
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 5 ] KAFKA — JUMLAH PESAN${RESET}"
echo "──────────────────────────────────────────────────────"
for topic in crypto-api crypto-rss; do
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka:29092 \
        --topic "$topic" \
        --time -1 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}')
    if [ "${count:-0}" -gt 0 ]; then
        ok "$topic → ${BOLD}$count pesan${RESET} tersimpan"
    else
        fail "$topic → 0 pesan (producer tidak berhasil kirim data)"
    fi
done
echo ""

# ─────────────────────────────────────────────
# 6. HDFS STATUS
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 6 ] HDFS — DATA TERSIMPAN${RESET}"
echo "──────────────────────────────────────────────────────"
for path in /data/crypto/api /data/crypto/rss; do
    count=$(docker exec namenode hdfs dfs -ls "$path" 2>/dev/null | grep -c "^-")
    size=$(docker exec namenode hdfs dfs -du -s "$path" 2>/dev/null | awk '{print int($1/1024)" KB"}')
    if [ "${count:-0}" -gt 0 ]; then
        ok "$path → $count file | $size"
    else
        warn "$path → belum ada file"
    fi
done
echo ""

# ─────────────────────────────────────────────
# RINGKASAN & SARAN
# ─────────────────────────────────────────────
echo -e "${CYAN}${BOLD}[ 7 ] RINGKASAN & SARAN${RESET}"
echo "──────────────────────────────────────────────────────"

# Cek umur live_api.json
if [ -f "$DASH_DIR/live_api.json" ]; then
    mod_time=$(stat -c %Y "$DASH_DIR/live_api.json")
    age=$(( (NOW - mod_time) / 60 ))
    if [ "$age" -gt 10 ]; then
        echo -e "  ${RED}${BOLD}MASALAH TERDETEKSI:${RESET}"

        # Cek apakah ada data di Kafka
        api_count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list kafka:29092 --topic crypto-api --time -1 2>/dev/null \
            | awk -F: '{sum += $3} END {print sum+0}')

        if [ "${api_count:-0}" -eq 0 ]; then
            echo -e "  → ${YELLOW}Producer tidak bisa kirim ke Kafka${RESET}"
            echo -e "  → ${YELLOW}Kemungkinan: API crypto diblokir ISP saat ini${RESET}"
            echo -e "  ${BOLD}SOLUSI:${RESET} Coba ganti jaringan (WiFi lain / hotspot HP)"
        else
            echo -e "  → ${YELLOW}Data ada di Kafka tapi tidak sampai ke dashboard${RESET}"
            echo -e "  ${BOLD}SOLUSI:${RESET} Restart consumer: docker restart consumer-hdfs"
        fi
    else
        ok "Semua normal! Data fresh, dashboard update otomatis."
    fi
fi

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════${RESET}"
echo ""
