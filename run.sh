#!/bin/sh
# run.sh — entrypoint Gold Intelligence HA Addon
# Legge /data/options.json (scritto da HA Supervisor) senza bashio

CONFIG=/data/options.json

# Helper: leggi campo da options.json
get_option() {
    python3 -c "
import json, sys
try:
    with open('${CONFIG}') as f:
        data = json.load(f)
    val = data.get('$1', '$2')
    print(val if val else '$2')
except:
    print('$2')
"
}

# Leggi configurazione
ANTHROPIC_API_KEY=$(get_option "anthropic_api_key" "")
PERPLEXITY_API_KEY=$(get_option "perplexity_api_key" "")
SCORE_THRESHOLD=$(get_option "score_threshold" "30")
ENGINE_MODE=$(get_option "engine_mode" "auto")

export ANTHROPIC_API_KEY
export PERPLEXITY_API_KEY
export SCORE_THRESHOLD
export ENGINE_MODE

echo "[Gold Intelligence] Avvio..."
echo "[Gold Intelligence] engine=${ENGINE_MODE} threshold=±${SCORE_THRESHOLD}"

if [ -n "${PERPLEXITY_API_KEY}" ]; then
    echo "[Gold Intelligence] Perplexity key rilevata → dual engine"
else
    echo "[Gold Intelligence] Nessuna Perplexity key → Claude + web search"
fi

exec python3 /app/main.py
