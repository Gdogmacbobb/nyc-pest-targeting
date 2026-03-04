#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash download_nyc_data.sh <output_dir>
# Optional:
#   export NYC_OPEN_DATA_APP_TOKEN="your_token"

OUT_DIR="${1:-data}"
mkdir -p "$OUT_DIR"

APP_TOKEN="${NYC_OPEN_DATA_APP_TOKEN:-}"

CURL_HEADERS=()
if [[ -n "$APP_TOKEN" ]]; then
  CURL_HEADERS=(-H "X-App-Token: $APP_TOKEN")
else
  echo "WARNING: NYC_OPEN_DATA_APP_TOKEN not set; downloads may be rate-limited."
fi

download() {
  local url="$1"
  local out="$2"
  echo "Downloading: $out"
  curl -fL --retry 3 --retry-delay 3 "${CURL_HEADERS[@]}" -o "$out" "$url"
}

# Datasets (Socrata):
# HPD Registration Contacts: feu5-w2e2
# HPD Registrations:         tesw-yqqr
# PLUTO:                     64uk-42ks
# HPD Violations:            wvxf-dwi5

download "https://data.cityofnewyork.us/resource/feu5-w2e2.csv?\$limit=800000"  "$OUT_DIR/hpd_contacts.csv"
download "https://data.cityofnewyork.us/resource/tesw-yqqr.csv?\$limit=300000"  "$OUT_DIR/hpd_registrations.csv"
download "https://data.cityofnewyork.us/resource/64uk-42ks.csv?\$limit=1200000" "$OUT_DIR/pluto.csv"
download "https://data.cityofnewyork.us/resource/wvxf-dwi5.csv?\$limit=1200000" "$OUT_DIR/hpd_violations.csv"

echo "Done. Files in: $OUT_DIR"
ls -lh "$OUT_DIR"
