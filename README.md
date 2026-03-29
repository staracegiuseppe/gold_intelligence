# Gold Intelligence — Home Assistant Add-on

Sistema di analisi mercato oro (XAU/USD) con Smart Money Score e Claude AI.

## Installazione

1. HA → **Impostazioni → Add-on Store → ⋮ → Repositories**
2. Aggiungi: `https://github.com/staracegiuseppe/gold_intelligence`
3. Installa **Gold Intelligence**
4. Configura `anthropic_api_key` nella tab Configurazione
5. Avvia → il pannello appare nella sidebar

## Struttura repo

```
repository.yaml
gold_intelligence/
├── addon.yaml        ← manifest HA
├── Dockerfile
├── run.sh
├── gold_engine.py
├── main.py
├── index.html
└── requirements.txt
```
