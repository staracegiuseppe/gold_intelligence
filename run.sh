#!/bin/sh
# Legge /data/options.json (scritto da HA Supervisor prima dell'avvio)

OPTIONS=/data/options.json

get_opt() {
    python3 -c "
import json, sys
try:
    d = json.load(open('${OPTIONS}'))
    v = d.get('$1', '$2')
    print(v if v else '$2')
except:
    print('$2')
"
}

export ANTHROPIC_API_KEY=$(get_opt "anthropic_api_key" "")
export PERPLEXITY_API_KEY=$(get_opt "perplexity_api_key" "")
export SCORE_THRESHOLD=$(get_opt "score_threshold" "30")
export ENGINE_MODE=$(get_opt "engine_mode" "auto")

echo "[Gold Intelligence] engine=${ENGINE_MODE} threshold=±${SCORE_THRESHOLD}"
[ -n "${PERPLEXITY_API_KEY}" ] \
    && echo "[Gold Intelligence] Dual engine attivo" \
    || echo "[Gold Intelligence] Claude + web search"

exec python3 /app/main.py
