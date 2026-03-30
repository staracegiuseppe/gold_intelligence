import json
import logging
import threading
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from ai_validation import enrich_signals
from backtest_engine import run_backtest_batch
from market_data import fetch_all_snapshots, fetch_snapshot
from signal_engine import build_quant_signal
from mailer import send_report


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("scanner")

VERSION = "2.0.0"


def load_options() -> Dict[str, Any]:
    p = Path("/data/options.json")
    if p.exists():
        try:
            opts = json.loads(p.read_text(encoding="utf-8"))
            log.info("[CONFIG] loaded %s", str(p))
            return opts
        except Exception as e:
            log.warning("[CONFIG] /data/options.json parse error: %s", e)

    return {
        "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY", ""),
        "perplexity_api_key": os.getenv("PERPLEXITY_API_KEY", ""),
        "score_threshold": int(os.getenv("SCORE_THRESHOLD", "35")),
        "scheduler_interval_minutes": int(os.getenv("SCHEDULER_MINUTES", "60")),
        "scheduler_enabled": True,
        "email_enabled": False,
        "email_to": os.getenv("EMAIL_TO", ""),
        "email_from": os.getenv("EMAIL_FROM", ""),
        "smtp_host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port": int(os.getenv("SMTP_PORT", "587")),
        "smtp_user": os.getenv("SMTP_USER", ""),
        "smtp_password": os.getenv("SMTP_PASSWORD", ""),
        "smtp_tls": True,
        "email_min_score": int(os.getenv("EMAIL_MIN_SCORE", "40")),
    }


OPTIONS = load_options()

CLAUDE_KEY = OPTIONS.get("anthropic_api_key", "") or os.getenv("ANTHROPIC_API_KEY", "")
SCORE_THRESHOLD = int(OPTIONS.get("score_threshold", 35))
SCHEDULER_MINUTES = int(OPTIONS.get("scheduler_interval_minutes", 60))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled", True))

BIND_HOST = os.getenv("BIND_HOST", "127.0.0.1")
PORT = int(os.getenv("INGRESS_PORT", "8099"))


WATCHLIST_PATH = Path(__file__).parent / "assets.json"
if not WATCHLIST_PATH.exists():
    raise RuntimeError(f"Missing assets config: {WATCHLIST_PATH}")

WATCHLIST_RAW = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
WATCHLIST: List[Dict[str, Any]] = [a for a in WATCHLIST_RAW if a.get("enabled", True)]
ASSET_META_BY_SYMBOL: Dict[str, Dict[str, Any]] = {a["symbol"]: a for a in WATCHLIST}

if not WATCHLIST:
    raise RuntimeError("assets.json has no enabled assets")


state: Dict[str, Any] = {
    "last_run": None,
    "next_run": None,
    "running": False,
    "results": [],
    "signals": [],
    "backtests": {},
    "email_last": None,
    "email_ok": None,
}


def _no_data_signal(meta: Dict[str, Any], *, now_iso: str) -> Dict[str, Any]:
    return {
        "symbol": meta.get("symbol", ""),
        "name": meta.get("name", ""),
        "market": meta.get("market", ""),
        "asset_type": meta.get("asset_type", ""),
        "action": "NO_DATA",
        "confidence": 0,
        "score_breakdown": [],
        "reasons": ["NO_DATA (missing or insufficient OHLCV indicators)"],
        "price": None,
        "indicators": {"missing": True},
        "entry": None,
        "stop_loss": None,
        "take_profit": None,
        "risk_reward": None,
        "has_real_data": False,
        "timestamp": now_iso,
        "score": 0,
    }


def _scan_once() -> List[Dict[str, Any]]:
    now = datetime.utcnow()
    now_iso = now.isoformat() + "Z"
    symbols = [a["symbol"] for a in WATCHLIST]

    snapshots = fetch_all_snapshots(symbols, period="2y", interval="1d")
    signals: List[Dict[str, Any]] = []

    for meta in WATCHLIST:
        sym = meta["symbol"]
        snap = snapshots.get(sym)
        if snap is None:
            signals.append(_no_data_signal(meta, now_iso=now_iso))
            continue
        sig = build_quant_signal(snap, meta)
        # Ensure metadata is always present.
        sig["name"] = meta.get("name", sig.get("name", ""))
        sig["market"] = meta.get("market", sig.get("market", ""))
        sig["asset_type"] = meta.get("asset_type", sig.get("asset_type", ""))
        sig["has_real_data"] = bool(snap.get("has_real_data", True))
        if not sig.get("timestamp"):
            sig["timestamp"] = now_iso
        signals.append(sig)

    # Optional AI enrichment (top BUY/SELL only). Quant direction and SL/TP remain unchanged.
    ai_inputs = [s for s in signals if s.get("action") in ("BUY", "SELL") and abs(float(s.get("score", 0))) >= SCORE_THRESHOLD]
    ai_inputs.sort(key=lambda x: abs(float(x.get("score", 0))), reverse=True)
    ai_inputs = ai_inputs[:10]  # extra cap before handing to ai_validation

    if CLAUDE_KEY and ai_inputs:
        enrich_signals(ai_inputs, anthropic_key=CLAUDE_KEY)

    # Sort: BUY/SELL by confidence desc, then WATCHLIST, then HOLD, then NO_DATA.
    def action_rank(a: str) -> int:
        if a == "BUY":
            return 0
        if a == "SELL":
            return 1
        if a == "WATCHLIST":
            return 2
        if a == "HOLD":
            return 3
        return 4

    signals.sort(key=lambda s: (action_rank(s.get("action", "NO_DATA")), -int(s.get("confidence", 0))))
    return signals


def run_scheduled_analysis() -> None:
    if state.get("running"):
        log.warning("[SCHEDULER] already running")
        return
    state["running"] = True
    try:
        log.info("[SCHEDULER] scan start symbols=%d", len(WATCHLIST))
        results = _scan_once()
        run_ts = datetime.utcnow().isoformat() + "Z"
        next_ts = (datetime.utcnow() + timedelta(minutes=SCHEDULER_MINUTES)).isoformat() + "Z"
        state.update({"results": results, "signals": results, "last_run": run_ts, "next_run": next_ts, "running": False})

        # Optional email (now uses quant signal fields).
        if OPTIONS.get("email_enabled"):
            try:
                ok = send_report(results, run_ts, next_ts, OPTIONS)
                state["email_last"] = run_ts
                state["email_ok"] = ok
            except Exception as e:
                log.error("[EMAIL] %s", e)
                state["email_ok"] = False

    finally:
        state["running"] = False


def scheduler_loop() -> None:
    run_scheduled_analysis()
    while True:
        time.sleep(SCHEDULER_MINUTES * 60)
        if SCHEDULER_ENABLED:
            run_scheduled_analysis()


app = FastAPI(title="Multi-Market Intelligence", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


async def _html():
    for p in [Path("/app/index.html"), Path(__file__).parent / "index.html"]:
        if p.exists():
            return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html non trovato</h1>", 404)


@app.get("/", response_class=HTMLResponse)
async def root():
    return await _html()


@app.get("/index.html", response_class=HTMLResponse)
async def idx():
    return await _html()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": VERSION,
        "anthropic_key": bool(CLAUDE_KEY),
        "score_threshold": SCORE_THRESHOLD,
        "scheduler_minutes": SCHEDULER_MINUTES,
        "scheduler_enabled": SCHEDULER_ENABLED,
        "enabled_assets": len(WATCHLIST),
        "running": bool(state.get("running")),
        "last_run": state.get("last_run"),
        "next_run": state.get("next_run"),
    }


@app.get("/api/config")
async def config():
    return {
        "version": VERSION,
        "score_threshold": SCORE_THRESHOLD,
        "has_anthropic": bool(CLAUDE_KEY),
        "scheduler_minutes": SCHEDULER_MINUTES,
        "scheduler_enabled": SCHEDULER_ENABLED,
        "email_enabled": bool(OPTIONS.get("email_enabled")),
        "email_to": OPTIONS.get("email_to", ""),
        "smtp_host": OPTIONS.get("smtp_host", ""),
        "smtp_port": OPTIONS.get("smtp_port", 587),
        "trading_note": "quant signals only (no gold-only logic)",
        "assets_count": len(WATCHLIST),
    }


@app.get("/api/assets")
async def get_assets():
    return {"assets": WATCHLIST, "count": len(WATCHLIST)}


@app.get("/api/signals")
async def get_signals():
    results = state.get("results") or []
    return {
        "last_run": state.get("last_run"),
        "next_run": state.get("next_run"),
        "running": bool(state.get("running")),
        "count": len(results),
        "signals": results,
        "active_signals": [s for s in results if s.get("action") in ("BUY", "SELL")],
    }


@app.get("/api/signals/{symbol}")
async def get_signal(symbol: str):
    results = state.get("results") or []
    for s in results:
        if s.get("symbol") == symbol:
            return s

    meta = ASSET_META_BY_SYMBOL.get(symbol)
    if not meta:
        raise HTTPException(404, "symbol not in enabled watchlist")

    snap = fetch_snapshot(symbol, period="2y", interval="1d")
    if snap is None:
        return _no_data_signal(meta, now_iso=datetime.utcnow().isoformat() + "Z")

    sig = build_quant_signal(snap, meta)
    sig["has_real_data"] = bool(snap.get("has_real_data", True))
    return sig


class BacktestRequest(BaseModel):
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    start: Optional[str] = None
    end: Optional[str] = None
    period: str = "3y"
    interval: str = "1d"
    initial_capital: float = 10000.0
    fees_bps: float = 5.0
    slippage_bps: float = 2.0
    max_holding_days: int = 10
    allow_short: bool = True


@app.post("/api/backtest")
async def api_backtest(req: BacktestRequest):
    symbols = req.symbols or ([req.symbol] if req.symbol else [])
    if not symbols:
        raise HTTPException(400, "provide symbol or symbols")
    # Keep only enabled watchlist symbols.
    symbols = [s for s in symbols if s in ASSET_META_BY_SYMBOL]
    if not symbols:
        raise HTTPException(400, "no valid enabled symbols")

    metas = {s: ASSET_META_BY_SYMBOL[s] for s in symbols}
    try:
        res = run_backtest_batch(
            symbols,
            metas,
            start=req.start,
            end=req.end,
            period=req.period,
            interval=req.interval,
            initial_capital=req.initial_capital,
            fees_bps=req.fees_bps,
            slippage_bps=req.slippage_bps,
            max_holding_days=req.max_holding_days,
            allow_short=req.allow_short,
        )
    except Exception as e:
        raise HTTPException(500, str(e))

    # Cache per symbol.
    for s in symbols:
        state["backtests"][s] = res.get("per_symbol", {}).get(s)

    return res


@app.get("/api/backtest/{symbol}")
async def get_backtest(symbol: str):
    if symbol not in ASSET_META_BY_SYMBOL:
        raise HTTPException(404, "symbol not in enabled watchlist")
    bt = state.get("backtests", {}).get(symbol)
    if not bt:
        raise HTTPException(404, "backtest not found (run /api/backtest first)")
    return bt


@app.get("/api/scheduled")
async def scheduled_alias():
    # Backward compatibility with the old frontend's scheduler tab.
    results = state.get("results") or []
    return {
        "last_run": state.get("last_run"),
        "next_run": state.get("next_run"),
        "running": bool(state.get("running")),
        "count": len(results),
        "results": results,
        "email_last": state.get("email_last"),
        "email_ok": state.get("email_ok"),
    }


@app.post("/api/scheduled/refresh")
async def refresh():
    if state.get("running"):
        return {"status": "already_running"}
    threading.Thread(target=run_scheduled_analysis, daemon=True).start()
    return {"status": "started"}


if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    log.info("[STARTUP] %s %s", "Multi-Market Intelligence", VERSION)
    log.info("[STARTUP] %s:%d anthropic=%s", BIND_HOST, PORT, "OK" if CLAUDE_KEY else "MANCANTE")
    uvicorn.run("main:app", host=BIND_HOST, port=PORT, log_level="warning")

