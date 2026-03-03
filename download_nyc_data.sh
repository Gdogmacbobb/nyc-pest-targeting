{\rtf1\ansi\ansicpg1252\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 #!/usr/bin/env bash\
# =============================================================================\
# NYC Open Data \'97 Raw Source Downloader\
# Run this script in an environment WITH network access to data.cityofnewyork.us\
# before running nyc_live_pipeline.py\
# =============================================================================\
set -euo pipefail\
\
TOKEN="$\{NYC_OPEN_DATA_APP_TOKEN:-\}"\
DEST_DIR="$\{1:-.\}"\
\
if [[ -z "$TOKEN" ]]; then\
  echo "WARNING: NYC_OPEN_DATA_APP_TOKEN not set \'97 downloads will be rate-limited."\
  echo "  Get a free token at: https://data.cityofnewyork.us/profile/app_tokens"\
  HEADER_ARGS=()\
else\
  echo "Using App Token: $\{TOKEN:0:8\}..."\
  HEADER_ARGS=(-H "X-App-Token: $TOKEN")\
fi\
\
mkdir -p "$DEST_DIR"\
\
echo ""\
echo "=== Downloading HPD Registration Contacts (feu5-w2e2) ==="\
echo "    Expected ~600K rows, ~150 MB"\
curl -L --fail --retry 3 --retry-delay 5 \\\
  "$\{HEADER_ARGS[@]\}" \\\
  -o "$DEST_DIR/hpd_contacts.csv" \\\
  "https://data.cityofnewyork.us/resource/feu5-w2e2.csv?\\$limit=750000"\
echo "    Saved: $DEST_DIR/hpd_contacts.csv ($(wc -l < "$DEST_DIR/hpd_contacts.csv") rows)"\
\
echo ""\
echo "=== Downloading HPD Multiple Dwelling Registrations (tesw-yqqr) ==="\
echo "    Expected ~150K rows, ~30 MB"\
curl -L --fail --retry 3 --retry-delay 5 \\\
  "$\{HEADER_ARGS[@]\}" \\\
  -o "$DEST_DIR/hpd_registrations.csv" \\\
  "https://data.cityofnewyork.us/resource/tesw-yqqr.csv?\\$limit=250000"\
echo "    Saved: $DEST_DIR/hpd_registrations.csv ($(wc -l < "$DEST_DIR/hpd_registrations.csv") rows)"\
\
echo ""\
echo "=== Downloading PLUTO latest (64uk-42ks) ==="\
echo "    Expected ~860K rows, ~600 MB"\
curl -L --fail --retry 3 --retry-delay 5 \\\
  "$\{HEADER_ARGS[@]\}" \\\
  -o "$DEST_DIR/pluto.csv" \\\
  "https://data.cityofnewyork.us/resource/64uk-42ks.csv?\\$limit=1000000"\
echo "    Saved: $DEST_DIR/pluto.csv ($(wc -l < "$DEST_DIR/pluto.csv") rows)"\
\
echo ""\
echo "=== Downloading HPD Violations (wvxf-dwi5) [Phase 2] ==="\
echo "    Expected ~1M rows, ~400 MB \'97 this may take several minutes"\
curl -L --fail --retry 3 --retry-delay 5 \\\
  "$\{HEADER_ARGS[@]\}" \\\
  -o "$DEST_DIR/hpd_violations.csv" \\\
  "https://data.cityofnewyork.us/resource/wvxf-dwi5.csv?\\$limit=1200000"\
echo "    Saved: $DEST_DIR/hpd_violations.csv ($(wc -l < "$DEST_DIR/hpd_violations.csv") rows)"\
\
echo ""\
echo "=== All downloads complete ==="\
ls -lh "$DEST_DIR"/*.csv\
}