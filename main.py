# main.py - Gold Intelligence v1.5.0
import os, json, logging, threading, time, random
from pathlib    import Path
from datetime   import datetime, timedelta
from fastapi    import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses       import HTMLResponse
from pydantic   import BaseModel
import requests, uvicorn
from gold_engine  import compute_score, enrich_signals, effective_mode
from market_data  import fetch_all
from mailer       import send_report
from signal_engine import run_full_pipeline, is_trading_hours

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("gold_addon")


def load_options():
    p = Path("/data/options.json")
    if p.exists():
        try:
            opts = json.load(open(p))
            log.info("[CONFIG] da /data/options.json")
            return opts
        except Exception as e:
            log.warning("[CONFIG] errore: " + str(e))
    log.info("[CONFIG] da env vars")
    return {
        "anthropic_api_key":          os.getenv("ANTHROPIC_API_KEY",""),
        "perplexity_api_key":         os.getenv("PERPLEXITY_API_KEY",""),
        "score_threshold":            int(os.getenv("SCORE_THRESHOLD","30")),
        "engine_mode":                os.getenv("ENGINE_MODE","auto"),
        "scheduler_interval_minutes": int(os.getenv("SCHEDULER_MINUTES","60")),
        "scheduler_enabled":          True,
        "email_enabled":              False,
        "email_to":   os.getenv("EMAIL_TO",""),
        "email_from": os.getenv("EMAIL_FROM",""),
        "smtp_host":  os.getenv("SMTP_HOST","smtp.gmail.com"),
        "smtp_port":  int(os.getenv("SMTP_PORT","587")),
        "smtp_user":  os.getenv("SMTP_USER",""),
        "smtp_password": os.getenv("SMTP_PASSWORD",""),
        "smtp_tls":   True,
        "email_min_score": 40,
    }


OPTIONS = load_options()
if OPTIONS.get("anthropic_api_key"):  os.environ["ANTHROPIC_API_KEY"]  = OPTIONS["anthropic_api_key"]
if OPTIONS.get("perplexity_api_key"): os.environ["PERPLEXITY_API_KEY"] = OPTIONS["perplexity_api_key"]

SCORE_THRESHOLD   = int(OPTIONS.get("score_threshold",30))
ENGINE_MODE       = OPTIONS.get("engine_mode","auto")
SCHEDULER_MINUTES = int(OPTIONS.get("scheduler_interval_minutes",60))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled",True))
BIND_HOST         = os.getenv("BIND_HOST","127.0.0.1")
PORT              = int(os.getenv("INGRESS_PORT","8099"))
CLAUDE_KEY        = os.getenv("ANTHROPIC_API_KEY","")
PPLX_KEY          = os.getenv("PERPLEXITY_API_KEY","")

ASSETS = [
    {"symbol":"GLD",  "name":"SPDR Gold ETF",    "type":"etf",   "sector":"gold"},
    {"symbol":"IAU",  "name":"iShares Gold ETF",  "type":"etf",   "sector":"gold"},
    {"symbol":"SPY",  "name":"S&P 500 ETF",       "type":"etf",   "sector":"equity"},
    {"symbol":"QQQ",  "name":"Nasdaq 100 ETF",    "type":"etf",   "sector":"tech"},
    {"symbol":"XLE",  "name":"Energy Select ETF", "type":"etf",   "sector":"energy"},
    {"symbol":"TLT",  "name":"20Y Treasury ETF",  "type":"etf",   "sector":"bond"},
    {"symbol":"AAPL", "name":"Apple Inc",         "type":"stock", "sector":"tech"},
    {"symbol":"NVDA", "name":"NVIDIA Corp",       "type":"stock", "sector":"tech"},
    {"symbol":"MSFT", "name":"Microsoft Corp",    "type":"stock", "sector":"tech"},
    {"symbol":"XOM",  "name":"ExxonMobil Corp",   "type":"stock", "sector":"energy"},
]

sched = {
    "last_run":None,"next_run":None,"running":False,
    "signals":[],"results":[],
    "email_last":None,"email_ok":None,
}


def build_smart_money(asset, tech):
    if not tech:
        random.seed(hash(asset["symbol"] + datetime.now().strftime("%Y%m%d%H")))
        b = {"gold":0.3,"tech":0.1,"energy":-0.1,"bond":-0.2,"equity":0.05}.get(asset["sector"],0)
        return {
            "etf_flows": round(random.gauss(200+b*500,150),1),
            "cot_positioning": round(random.gauss(80000+b*100000,50000)),
            "real_yields": round(random.gauss(-0.2+b*0.3,0.3),2),
            "usd_trend": random.choice(["down","down","flat","up"]),
            "gold_trend": "up" if b>0 else "down" if b<-0.1 else "sideways",
            "macro_event":"Fed hold expected", **asset,
        }
    rsi=tech["rsi"]; bb=tech["bollinger"]["position"]
    cross=tech["ma"]["cross_signal"]; vol=tech["volume"]["ratio_pct"]
    p5=tech["performance"].get("5d",0) or 0
    return {
        "etf_flows": round((vol-100)*5+p5*20,1),
        "cot_positioning": round((rsi-50)*2000+(bb-50)*1500),
        "real_yields": -0.2 if "rialzista" in cross else 0.3,
        "usd_trend": "down" if "rialzista" in cross else "up",
        "gold_trend": "up" if p5>1 else "down" if p5<-1 else "sideways",
        "macro_event":"Fed hold expected", **asset,
    }


def run_scheduled_analysis():
    global sched
    if sched["running"]:
        log.warning("[SCHEDULER] gia in esecuzione - skip")
        return
    sched["running"] = True
    log.info("[SCHEDULER] ========== AVVIO " + datetime.now().strftime("%H:%M:%S") + " ==========")
    log.info("[SCHEDULER] asset=" + str(len(ASSETS))
             + "  threshold=+-" + str(SCORE_THRESHOLD)
             + "  claude=" + ("OK" if CLAUDE_KEY else "MANCANTE")
             + "  perplexity=" + ("OK" if PPLX_KEY else "ASSENTE")
             + "  finestra=" + ("APERTA" if is_trading_hours() else "CHIUSA"))

    symbols   = [a["symbol"] for a in ASSETS]
    tech_data = fetch_all(symbols)
    log.info("[SCHEDULER] yfinance: "
             + str(len(tech_data)) + "/" + str(len(symbols)) + " OK  ["
             + " ".join(s+("v" if s in tech_data else "x") for s in symbols) + "]")

    score_map = {}
    for asset in ASSETS:
        sym=asset["symbol"]; tech=tech_data.get(sym)
        d=build_smart_money(asset,tech); sd=compute_score(d); sc=sd["score"]
        row={
            "symbol":sym,"name":asset["name"],"type":asset["type"],"sector":asset["sector"],
            "score":sc,"breakdown":sd["breakdown"],"signals":enrich_signals(d),
            "tech":tech,"analysis":None,"triggered":abs(sc)>SCORE_THRESHOLD,
            "timestamp":datetime.utcnow().isoformat()+"Z","has_real_data":tech is not None,
        }
        score_map[sym] = {"asset":asset,"data":d,"score":sc,"score_data":sd,"row":row}

    signals = run_full_pipeline(
        assets=ASSETS, tech_data=tech_data, score_map=score_map,
        claude_key=CLAUDE_KEY, pplx_key=PPLX_KEY, threshold=SCORE_THRESHOLD,
    )

    results=[sm["row"] for sm in score_map.values()]
    results.sort(key=lambda r:abs(r["score"]),reverse=True)
    run_ts=datetime.utcnow().isoformat()+"Z"
    next_ts=(datetime.utcnow()+timedelta(minutes=SCHEDULER_MINUTES)).isoformat()+"Z"
    sched.update({"signals":signals,"results":results,"last_run":run_ts,"next_run":next_ts,"running":False})

    active=[s for s in signals if s["action"] in ("BUY","SELL")]
    log.info("[SCHEDULER] ========== DONE: " + str(len(active)) + " segnali attivi ==========")
    for s in active[:5]:
        log.info("[SCHEDULER] >>> " + s["symbol"]
                 + " " + s["action"]
                 + "  entry=" + str(s.get("entry","?"))
                 + "  SL=" + str(s.get("stop_loss","?"))
                 + "  TP=" + str(s.get("take_profit","?"))
                 + "  RR=1:" + str(s.get("risk_reward","?"))
                 + "  conf=" + str(s.get("confidence","?")) + "%"
                 + "  pplx=" + str(s.get("pplx_verdict","?")))

    if OPTIONS.get("email_enabled"):
        try:
            ok=send_report(results,run_ts,next_ts,OPTIONS)
            sched["email_last"]=run_ts; sched["email_ok"]=ok
            log.info("[EMAIL] " + ("OK" if ok else "FALLITA"))
        except Exception as e:
            log.error("[EMAIL] " + str(e)); sched["email_ok"]=False


def scheduler_loop():
    log.info("[SCHEDULER] thread avviato - intervallo " + str(SCHEDULER_MINUTES) + " min")
    run_scheduled_analysis()
    while True:
        time.sleep(SCHEDULER_MINUTES*60)
        if SCHEDULER_ENABLED: run_scheduled_analysis()


app = FastAPI(title="Gold Intelligence", version="1.5.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class MarketData(BaseModel):
    etf_flows:float; cot_positioning:float; real_yields:float
    usd_trend:str; gold_trend:str; macro_event:str; mode:str="auto"

async def _html():
    for p in [Path("/app/index.html"), Path(__file__).parent/"index.html"]:
        if p.exists(): return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html non trovato</h1>",404)

@app.get("/",           response_class=HTMLResponse)
async def root():  return await _html()
@app.get("/index.html", response_class=HTMLResponse)
async def idx():   return await _html()

@app.get("/health")
async def health():
    return {"status":"ok","version":"1.5.0","engine_mode":ENGINE_MODE,
            "effective_mode":effective_mode(ENGINE_MODE),
            "anthropic_key":bool(CLAUDE_KEY),"perplexity_key":bool(PPLX_KEY),
            "score_threshold":SCORE_THRESHOLD,"scheduler_minutes":SCHEDULER_MINUTES,
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "trading_window":is_trading_hours(),"bind":BIND_HOST}

@app.get("/api/config")
async def config():
    return {"engine_mode":ENGINE_MODE,"score_threshold":SCORE_THRESHOLD,
            "has_anthropic":bool(CLAUDE_KEY),"has_perplexity":bool(PPLX_KEY),
            "scheduler_minutes":SCHEDULER_MINUTES,"scheduler_enabled":SCHEDULER_ENABLED,
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "email_to":OPTIONS.get("email_to",""),"smtp_host":OPTIONS.get("smtp_host",""),
            "smtp_port":OPTIONS.get("smtp_port",587),"email_min_score":OPTIONS.get("email_min_score",40),
            "trading_window":is_trading_hours()}

@app.get("/api/signals")
async def get_signals():
    signals=sched.get("signals",[])
    active=[s for s in signals if s.get("action") in ("BUY","SELL")]
    return {"last_run":sched["last_run"],"next_run":sched["next_run"],
            "trading_window":is_trading_hours(),"active_signals":len(active),
            "buy": [s for s in active if s["action"]=="BUY"],
            "sell":[s for s in active if s["action"]=="SELL"],
            "hold":[s for s in signals  if s.get("action")=="HOLD"],
            "all": signals}

@app.get("/api/scheduled")
async def scheduled():
    clean=[]
    for r in sched.get("results",[]):
        row={k:v for k,v in r.items() if k!="tech"}
        if r.get("tech"):
            t=r["tech"]
            row["tech_summary"]={
                "price":t["last_price"],"change":t["change_pct"],"rsi":t["rsi"],
                "bb_pos":t["bollinger"]["position"],"bb_signal":t["bollinger"]["signal"],
                "ma_cross":t["ma"]["cross_signal"],"macd":t["macd"]["trend"],
                "stoch":t["stochastic"]["signal"],"vol":t["volume"]["signal"],
                "perf_5d":t["performance"]["5d"],
                "support":t["support_res"]["support"],"resistance":t["support_res"]["resistance"],
            }
        clean.append(row)
    return {"last_run":sched["last_run"],"next_run":sched["next_run"],
            "running":sched["running"],"count":len(clean),"results":clean,
            "email_last":sched["email_last"],"email_ok":sched["email_ok"]}

@app.post("/api/score")
async def score_only(data:MarketData):
    d=data.model_dump(exclude={"mode"})
    return {"smart_money":compute_score(d),"signals":enrich_signals(d)}

@app.post("/api/analyze")
async def analyze(data:MarketData):
    from gold_engine import run_pipeline
    try:
        d=data.model_dump(exclude={"mode"}); mode=data.mode if data.mode!="auto" else ENGINE_MODE
        return run_pipeline(d,mode=mode)
    except EnvironmentError as e: raise HTTPException(400,str(e))
    except Exception as e: log.error("[ANALYZE] "+str(e)); raise HTTPException(500,str(e))

@app.post("/api/scheduled/refresh")
async def refresh():
    if sched["running"]: return {"status":"already_running"}
    threading.Thread(target=run_scheduled_analysis,daemon=True).start()
    return {"status":"started"}

@app.post("/api/email/test")
async def email_test():
    if not OPTIONS.get("email_enabled"): raise HTTPException(400,"Abilita email_enabled")
    if not sched.get("results"): raise HTTPException(400,"Avvia scheduler prima")
    try:
        ok=send_report(sched["results"],
                       sched["last_run"] or datetime.utcnow().isoformat()+"Z",
                       sched["next_run"] or "",OPTIONS)
        return {"status":"sent" if ok else "failed"}
    except Exception as e: raise HTTPException(500,str(e))

if __name__=="__main__":
    threading.Thread(target=scheduler_loop,daemon=True).start()
    log.info("[STARTUP] Gold Intelligence v1.5.0")
    log.info("[STARTUP] " + BIND_HOST + ":" + str(PORT)
             + "  Claude=" + ("OK" if CLAUDE_KEY else "MANCANTE")
             + "  Perplexity=" + ("OK" if PPLX_KEY else "non configurato"))
    uvicorn.run("main:app",host=BIND_HOST,port=PORT,log_level="warning")
