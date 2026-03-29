# main.py — Gold Intelligence Add-on
# Legge config da:
#   1. /data/options.json  (HA Supervisor — produzione)
#   2. env vars            (sviluppo locale / test)

import os
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

from gold_engine import run_pipeline, compute_score, enrich_signals, effective_mode

# ── Lettura config ─────────────────────────────────────────────────────────────
def load_options() -> dict:
    """
    Legge /data/options.json (HA) con fallback a env vars.
    HA scrive sempre questo file prima di avviare l'addon.
    """
    options_path = Path("/data/options.json")
    if options_path.exists():
        try:
            with open(options_path) as f:
                opts = json.load(f)
            logging.info("Config caricata da /data/options.json")
            return opts
        except Exception as e:
            logging.warning(f"Errore lettura options.json: {e} — uso env vars")

    # Fallback env vars (sviluppo locale)
    logging.info("Config caricata da env vars")
    return {
        "anthropic_api_key":  os.getenv("ANTHROPIC_API_KEY", ""),
        "perplexity_api_key": os.getenv("PERPLEXITY_API_KEY", ""),
        "score_threshold":    int(os.getenv("SCORE_THRESHOLD", "30")),
        "engine_mode":        os.getenv("ENGINE_MODE", "auto"),
    }

OPTIONS = load_options()

# Inietta le key come env vars (gold_engine.py le legge con os.getenv)
if OPTIONS.get("anthropic_api_key"):
    os.environ["ANTHROPIC_API_KEY"] = OPTIONS["anthropic_api_key"]
if OPTIONS.get("perplexity_api_key"):
    os.environ["PERPLEXITY_API_KEY"] = OPTIONS["perplexity_api_key"]

SCORE_THRESHOLD = int(OPTIONS.get("score_threshold", 30))
ENGINE_MODE     = OPTIONS.get("engine_mode", "auto")
PORT            = int(os.getenv("INGRESS_PORT", "8099"))

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold_addon")

# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Gold Intelligence", version="1.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Models ─────────────────────────────────────────────────────────────────────
class MarketData(BaseModel):
    etf_flows: float
    cot_positioning: float
    real_yields: float
    usd_trend: str
    gold_trend: str
    macro_event: str
    mode: str = "auto"

# ── Frontend ───────────────────────────────────────────────────────────────────
async def _html() -> HTMLResponse:
    for p in [Path("/app/index.html"), Path(__file__).parent / "index.html"]:
        if p.exists():
            return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html non trovato</h1>", status_code=404)

@app.get("/", response_class=HTMLResponse)
async def root(): return await _html()

@app.get("/index.html", response_class=HTMLResponse)
async def index(): return await _html()

# ── API ────────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":           "ok",
        "engine_mode":      ENGINE_MODE,
        "effective_mode":   effective_mode(ENGINE_MODE),
        "anthropic_key":    bool(os.getenv("ANTHROPIC_API_KEY")),
        "perplexity_key":   bool(os.getenv("PERPLEXITY_API_KEY")),
        "score_threshold":  SCORE_THRESHOLD,
    }

@app.get("/api/config")
async def config():
    return {
        "engine_mode":      ENGINE_MODE,
        "score_threshold":  SCORE_THRESHOLD,
        "has_anthropic":    bool(os.getenv("ANTHROPIC_API_KEY")),
        "has_perplexity":   bool(os.getenv("PERPLEXITY_API_KEY")),
    }

@app.post("/api/score")
async def score_only(data: MarketData):
    d = data.model_dump(exclude={"mode"})
    return {
        "smart_money": compute_score(d),
        "signals":     enrich_signals(d),
    }

@app.post("/api/analyze")
async def analyze(data: MarketData):
    try:
        d    = data.model_dump(exclude={"mode"})
        mode = data.mode if data.mode != "auto" else ENGINE_MODE
        return run_pipeline(d, mode=mode)
    except EnvironmentError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error(f"Pipeline error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── Start ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"Gold Intelligence avviato — porta {PORT}")
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
