#!/bin/sh
# run.sh - Market Analyze v2.4 - Tutte le API key esportate

# Rimuovi proxy che interferiscono con Yahoo Finance
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY no_proxy NO_PROXY
export NO_PROXY="*"

OPTIONS=/data/options.json

# Helper: legge una chiave da options.json
get_opt() {
    python3 -c "
import json,sys
try:
    d=json.load(open('${OPTIONS}'))
    v=d.get('$1','$2')
    print(str(v) if v is not None and str(v)!='' else '$2')
except:
    print('$2')
" 2>/dev/null || echo "$2"
}

# ── API Keys ──────────────────────────────────────────────────────────────────
export ANTHROPIC_API_KEY=$(get_opt "anthropic_api_key" "")
export PERPLEXITY_API_KEY=$(get_opt "perplexity_api_key" "")
export FRED_API_KEY=$(get_opt "fred_api_key" "")
export FMP_API_KEY=$(get_opt "fmp_api_key" "")
export EIA_API_KEY=$(get_opt "eia_api_key" "")

# ── Config ────────────────────────────────────────────────────────────────────
export SCORE_THRESHOLD=$(get_opt "score_threshold" "25")
export SCHEDULER_MINUTES=$(get_opt "scheduler_interval_minutes" "60")
export BIND_HOST="0.0.0.0"
export INGRESS_PORT="8099"

# ── Startup log ───────────────────────────────────────────────────────────────
echo "[Market Analyze v2.4] Avvio su ${BIND_HOST}:${INGRESS_PORT}"
echo "[Config] Claude=$([ -n "${ANTHROPIC_API_KEY}" ] && echo ON || echo MANCANTE)"
echo "[Config] Perplexity=$([ -n "${PERPLEXITY_API_KEY}" ] && echo ON || echo off)"
echo "[Config] FRED=$([ -n "${FRED_API_KEY}" ] && echo ON || echo off) | FMP=$([ -n "${FMP_API_KEY}" ] && echo ON || echo off) | EIA=$([ -n "${EIA_API_KEY}" ] && echo ON || echo off)"
echo "[Config] Scheduler ogni ${SCHEDULER_MINUTES}min | Finestra 08:00-23:30"

exec python3 /app/main.py
