#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-data}"
mkdir -p "$OUT_DIR"

APP_TOKEN="${NYC_OPEN_DATA_APP_TOKEN:-}"
CURL_HEADERS=()
if [[ -n "$APP_TOKEN" ]]; then
  CURL_HEADERS=(-H "X-App-Token: $APP_TOKEN")
else
  echo "WARNING: NYC_OPEN_DATA_APP_TOKEN not set; rate limits may apply."
fi

download() {
  local url="$1"
  local out="$2"
  echo "Downloading: $out"
  curl -fL --retry 8 --retry-delay 3 --retry-all-errors "${CURL_HEADERS[@]}" -o "$out" "$url"
}

download "https://data.cityofnewyork.us/resource/feu5-w2e2.csv?\$limit=800000"  "$OUT_DIR/hpd_contacts.csv"
download "https://data.cityofnewyork.us/resource/tesw-yqqr.csv?\$limit=300000"  "$OUT_DIR/hpd_registrations.csv"
download "https://data.cityofnewyork.us/resource/64uk-42ks.csv?\$limit=1200000" "$OUT_DIR/pluto.csv"
download "https://data.cityofnewyork.us/resource/wvxf-dwi5.csv?\$limit=1200000" "$OUT_DIR/hpd_violations.csv"

echo "Done. Files in: $OUT_DIR"
ls -lh "$OUT_DIR"
