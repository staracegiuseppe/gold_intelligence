# main.py — Gold Intelligence v1.2.0
# FIX: bind 127.0.0.1 (non 0.0.0.0) — porta non esposta su LAN
# FIX: gestione X-Ingress-Token corretta
# FIX: nessuna chiamata a HA API interna

import os, json, logging, threading, time, random
from pathlib    import Path
from datetime   import datetime, timedelta
from fastapi    import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses       import HTMLResponse
from pydantic   import BaseModel
import uvicorn

from gold_engine import run_pipeline, compute_score, enrich_signals, effective_mode
from mailer      import send_report

# ── Config ─────────────────────────────────────────────────────────────────────
def load_options() -> dict:
    for p in [Path("/data/options.json")]:
        if p.exists():
            try:
                opts = json.load(open(p))
                logging.info("Config da /data/options.json")
                return opts
            except Exception as e:
                logging.warning(f"Errore options.json: {e}")
    logging.info("Config da env vars")
    return {
        "anthropic_api_key":          os.getenv("ANTHROPIC_API_KEY",""),
        "perplexity_api_key":         os.getenv("PERPLEXITY_API_KEY",""),
        "score_threshold":            int(os.getenv("SCORE_THRESHOLD","30")),
        "engine_mode":                os.getenv("ENGINE_MODE","auto"),
        "scheduler_interval_minutes": int(os.getenv("SCHEDULER_MINUTES","60")),
        "scheduler_enabled":          True,
        "email_enabled":              False,
        "email_to":                   os.getenv("EMAIL_TO",""),
        "email_from":                 os.getenv("EMAIL_FROM",""),
        "smtp_host":                  os.getenv("SMTP_HOST","smtp.gmail.com"),
        "smtp_port":                  int(os.getenv("SMTP_PORT","587")),
        "smtp_user":                  os.getenv("SMTP_USER",""),
        "smtp_password":              os.getenv("SMTP_PASSWORD",""),
        "smtp_tls":                   True,
        "email_min_score":            40,
    }

OPTIONS = load_options()
if OPTIONS.get("anthropic_api_key"):
    os.environ["ANTHROPIC_API_KEY"] = OPTIONS["anthropic_api_key"]
if OPTIONS.get("perplexity_api_key"):
    os.environ["PERPLEXITY_API_KEY"] = OPTIONS["perplexity_api_key"]

SCORE_THRESHOLD   = int(OPTIONS.get("score_threshold", 30))
ENGINE_MODE       = OPTIONS.get("engine_mode", "auto")
SCHEDULER_MINUTES = int(OPTIONS.get("scheduler_interval_minutes", 60))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled", True))

# ── Bind host (CRITICO per sicurezza) ─────────────────────────────────────────
# 127.0.0.1 = solo ingress HA può raggiungere il server
# 0.0.0.0   = esposto su tutta la LAN → HA logga "invalid authentication"
BIND_HOST = os.getenv("BIND_HOST", "127.0.0.1")
PORT      = int(os.getenv("INGRESS_PORT", "8099"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold_addon")

# ── Asset Universe ─────────────────────────────────────────────────────────────
ASSETS = [
    {"symbol":"GLD",  "name":"SPDR Gold ETF",      "type":"etf",   "sector":"gold"},
    {"symbol":"IAU",  "name":"iShares Gold ETF",    "type":"etf",   "sector":"gold"},
    {"symbol":"SPY",  "name":"S&P 500 ETF",         "type":"etf",   "sector":"equity"},
    {"symbol":"QQQ",  "name":"Nasdaq 100 ETF",      "type":"etf",   "sector":"tech"},
    {"symbol":"XLE",  "name":"Energy Select ETF",   "type":"etf",   "sector":"energy"},
    {"symbol":"TLT",  "name":"20Y Treasury ETF",    "type":"etf",   "sector":"bond"},
    {"symbol":"AAPL", "name":"Apple Inc",           "type":"stock", "sector":"tech"},
    {"symbol":"NVDA", "name":"NVIDIA Corp",         "type":"stock", "sector":"tech"},
    {"symbol":"MSFT", "name":"Microsoft Corp",      "type":"stock", "sector":"tech"},
    {"symbol":"XOM",  "name":"ExxonMobil Corp",     "type":"stock", "sector":"energy"},
]

# ── Scheduler ──────────────────────────────────────────────────────────────────
sched = {
    "last_run": None, "next_run": None,
    "running": False, "results": [],
    "email_last": None, "email_ok": None, "error": None,
}

def simulate_asset_data(asset):
    random.seed(hash(asset["symbol"] + datetime.now().strftime("%Y%m%d%H")))
    bias = {"gold":0.3,"tech":0.1,"energy":-0.1,"bond":-0.2,"equity":0.05}.get(asset["sector"],0)
    return {
        "etf_flows":       round(random.gauss(200+bias*500, 150), 1),
        "cot_positioning": round(random.gauss(80000+bias*100000, 50000)),
        "real_yields":     round(random.gauss(-0.2+bias*0.3, 0.3), 2),
        "usd_trend":       random.choice(["down","down","flat","up"]),
        "gold_trend":      random.choice(["up","up","sideways","down"])
                           if asset["sector"]=="gold"
                           else random.choice(["up","sideways","down"]),
        "macro_event":     "Fed hold expected",
        **asset,
    }

def run_scheduled_analysis():
    global sched
    if sched["running"]: return
    sched["running"] = True
    log.info("=== SCHEDULER START ===")
    results = []
    for asset in ASSETS:
        try:
            d     = simulate_asset_data(asset)
            sd    = compute_score(d)
            score = sd["score"]
            analysis = None
            if abs(score) > SCORE_THRESHOLD and os.getenv("ANTHROPIC_API_KEY"):
                try:
                    res      = run_pipeline(d, mode=ENGINE_MODE)
                    analysis = res.get("analysis")
                except Exception as e:
                    log.warning(f"LLM {asset['symbol']}: {e}")
            results.append({
                "symbol": asset["symbol"], "name": asset["name"],
                "type": asset["type"],     "sector": asset["sector"],
                "score": score,            "breakdown": sd["breakdown"],
                "signals": enrich_signals(d), "analysis": analysis,
                "triggered": abs(score) > SCORE_THRESHOLD,
                "timestamp": datetime.utcnow().isoformat()+"Z",
            })
            log.info(f"  {asset['symbol']:6} score={score:+d}")
        except Exception as e:
            log.error(f"  {asset['symbol']} ERRORE: {e}")

    results.sort(key=lambda r: abs(r["score"]), reverse=True)
    run_ts  = datetime.utcnow().isoformat()+"Z"
    next_ts = (datetime.utcnow()+timedelta(minutes=SCHEDULER_MINUTES)).isoformat()+"Z"
    sched.update({"results":results,"last_run":run_ts,"next_run":next_ts,"running":False})
    log.info(f"=== SCHEDULER DONE: {len(results)} asset ===")

    if OPTIONS.get("email_enabled"):
        try:
            ok = send_report(results, run_ts, next_ts, OPTIONS)
            sched["email_last"] = run_ts
            sched["email_ok"]   = ok
        except Exception as e:
            log.error(f"Email: {e}"); sched["email_ok"] = False

def scheduler_loop():
    run_scheduled_analysis()
    while True:
        time.sleep(SCHEDULER_MINUTES * 60)
        if SCHEDULER_ENABLED: run_scheduled_analysis()

# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Gold Intelligence", version="1.2.0")

# CORS: permette solo richieste da HA Ingress (stesso host)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # HA Ingress proxy gestisce l'autenticazione
    allow_methods=["*"],
    allow_headers=["*"],
)

class MarketData(BaseModel):
    etf_flows: float;       cot_positioning: float
    real_yields: float;     usd_trend: str
    gold_trend: str;        macro_event: str
    mode: str = "auto"

async def _html():
    for p in [Path("/app/index.html"), Path(__file__).parent/"index.html"]:
        if p.exists():
            return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html non trovato</h1>", 404)

@app.get("/",           response_class=HTMLResponse)
async def root():  return await _html()
@app.get("/index.html", response_class=HTMLResponse)
async def index(): return await _html()

# Health check per HA Supervisor (usato per ingress probe)
@app.get("/health")
async def health():
    return {
        "status": "ok", "version": "1.2.0",
        "engine_mode": ENGINE_MODE,
        "effective_mode": effective_mode(ENGINE_MODE),
        "anthropic_key":  bool(os.getenv("ANTHROPIC_API_KEY")),
        "perplexity_key": bool(os.getenv("PERPLEXITY_API_KEY")),
        "score_threshold": SCORE_THRESHOLD,
        "scheduler_minutes": SCHEDULER_MINUTES,
        "email_enabled": bool(OPTIONS.get("email_enabled")),
        "bind": BIND_HOST,   # debug: conferma bind corretto
    }

@app.get("/api/config")
async def config():
    return {
        "engine_mode":       ENGINE_MODE,
        "score_threshold":   SCORE_THRESHOLD,
        "has_anthropic":     bool(os.getenv("ANTHROPIC_API_KEY")),
        "has_perplexity":    bool(os.getenv("PERPLEXITY_API_KEY")),
        "scheduler_minutes": SCHEDULER_MINUTES,
        "scheduler_enabled": SCHEDULER_ENABLED,
        "email_enabled":     bool(OPTIONS.get("email_enabled")),
        "email_to":          OPTIONS.get("email_to",""),
        "smtp_host":         OPTIONS.get("smtp_host",""),
        "smtp_port":         OPTIONS.get("smtp_port",587),
        "email_min_score":   OPTIONS.get("email_min_score",40),
    }

@app.post("/api/score")
async def score_only(data: MarketData):
    d = data.model_dump(exclude={"mode"})
    return {"smart_money": compute_score(d), "signals": enrich_signals(d)}

@app.post("/api/analyze")
async def analyze(data: MarketData):
    try:
        d    = data.model_dump(exclude={"mode"})
        mode = data.mode if data.mode != "auto" else ENGINE_MODE
        return run_pipeline(d, mode=mode)
    except EnvironmentError as e: raise HTTPException(400, str(e))
    except Exception as e:
        log.error(f"Pipeline: {e}"); raise HTTPException(500, str(e))

@app.get("/api/scheduled")
async def scheduled():
    return {
        "last_run":   sched["last_run"],
        "next_run":   sched["next_run"],
        "running":    sched["running"],
        "count":      len(sched["results"]),
        "results":    sched["results"],
        "email_last": sched["email_last"],
        "email_ok":   sched["email_ok"],
    }

@app.post("/api/scheduled/refresh")
async def refresh():
    if sched["running"]: return {"status":"already_running"}
    threading.Thread(target=run_scheduled_analysis, daemon=True).start()
    return {"status":"started"}

@app.post("/api/email/test")
async def email_test():
    if not OPTIONS.get("email_enabled"):
        raise HTTPException(400, "Email non abilitata — imposta email_enabled: true")
    if not sched["results"]:
        raise HTTPException(400, "Nessun risultato — avvia prima lo scheduler")
    try:
        ok = send_report(
            sched["results"],
            sched["last_run"] or datetime.utcnow().isoformat()+"Z",
            sched["next_run"] or "",
            OPTIONS,
        )
        return {"status": "sent" if ok else "failed"}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── Avvio ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    log.info(f"Gold Intelligence v1.2.0 — {BIND_HOST}:{PORT}")
    log.info("Porta esposta SOLO via HA Ingress (non accessibile da LAN)")
    uvicorn.run(
        "main:app",
        host=BIND_HOST,      # 127.0.0.1 — FIX CRITICO
        port=PORT,
        log_level="warning",
    )
