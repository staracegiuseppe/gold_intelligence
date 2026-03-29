#!/bin/sh
# run.sh — Gold Intelligence v1.2.0
# FIX: uvicorn bind su 127.0.0.1 (solo ingress HA, non esposto su LAN)

OPTIONS=/data/options.json

get_opt() {
    python3 -c "
import json, sys
try:
    d = json.load(open('${OPTIONS}'))
    v = d.get('$1', '$2')
    print(str(v) if v is not None and str(v) != '' else '$2')
except Exception as e:
    print('$2')
"
}

export ANTHROPIC_API_KEY=$(get_opt "anthropic_api_key" "")
export PERPLEXITY_API_KEY=$(get_opt "perplexity_api_key" "")
export SCORE_THRESHOLD=$(get_opt "score_threshold" "30")
export ENGINE_MODE=$(get_opt "engine_mode" "auto")
export SCHEDULER_MINUTES=$(get_opt "scheduler_interval_minutes" "60")

# CRITICO: bind su 127.0.0.1 NON 0.0.0.0
# HA Ingress fa da proxy — la porta non deve essere raggiungibile
# direttamente dalla LAN (altrimenti HA logga "invalid authentication")
export BIND_HOST="127.0.0.1"
export INGRESS_PORT=$(get_opt "ingress_port" "8099")

echo "[Gold Intelligence v1.2.0] Avvio..."
echo "[Gold Intelligence] Bind: ${BIND_HOST}:${INGRESS_PORT} (solo ingress HA)"
echo "[Gold Intelligence] Engine: ${ENGINE_MODE} | Soglia: ±${SCORE_THRESHOLD}"

if [ -n "${PERPLEXITY_API_KEY}" ]; then
    echo "[Gold Intelligence] Dual engine (Perplexity + Claude)"
else
    echo "[Gold Intelligence] Claude + web search"
fi

exec python3 /app/main.py
