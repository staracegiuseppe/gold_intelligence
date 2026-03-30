"""
ai_validation.py
Optional LLM enrichment for top candidate quantitative signals.

Hard rule: AI must NOT generate direction or SL/TP. It may only:
  - produce a short summary + risk flags
  - adjust confidence by [-5..+5]
  - provide a news_bias label
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger("ai_validation")

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

MAX_NEWS_ITEMS_PER_ASSET = int(os.getenv("MAX_NEWS_ITEMS_PER_ASSET", "3"))
MAX_LLM_ASSETS_PER_RUN = int(os.getenv("MAX_LLM_ASSETS_PER_RUN", "3"))
CONFIDENCE_ADJ_MIN = -5
CONFIDENCE_ADJ_MAX = 5

CACHE_TTL_MIN = int(os.getenv("AI_CACHE_TTL_MIN", "60"))
_cache: Dict[str, Dict[str, Any]] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _bucket(ts: Optional[datetime] = None) -> str:
    ts = ts or _utc_now()
    return ts.strftime("%Y%m%d%H")


def _ckey(symbol: str, action: str, b: str, snapshot_fingerprint: str) -> str:
    raw = f"{symbol}|{action}|{b}|{snapshot_fingerprint}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _truncate(s: str, max_chars: int = 90) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


def _dedupe_headlines(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("title") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def fetch_headlines(symbol: str, max_items: int = MAX_NEWS_ITEMS_PER_ASSET, max_age_days: int = 14) -> List[Dict[str, str]]:
    """
    Fetch headlines using yfinance without LLM calls.
    If news is missing, returns [].
    """
    try:
        import yfinance as yf

        t = yf.Ticker(symbol)
        news = getattr(t, "news", None) or []
        if not isinstance(news, list) or not news:
            return []

        now = _utc_now()
        items = []
        for n in news:
            title = n.get("title") or ""
            if not title:
                continue
            publisher = n.get("publisher") or n.get("source") or ""
            # yfinance uses providerPublishTime (unix seconds) in most cases.
            pts = n.get("providerPublishTime") or n.get("pubDate") or None
            dt = None
            if isinstance(pts, (int, float)):
                try:
                    dt = datetime.fromtimestamp(float(pts), tz=timezone.utc)
                except Exception:
                    dt = None
            if dt is None:
                # If no timestamp, still include but push to the end by using a very old date.
                dt = now - timedelta(days=max_age_days + 1)

            age_days = (now - dt).total_seconds() / 86400
            if age_days > max_age_days:
                continue

            items.append(
                {
                    "title": _truncate(title, 90),
                    "source": _truncate(str(publisher), 40),
                    "date": dt.date().isoformat(),
                }
            )

        items.sort(key=lambda x: x.get("date") or "", reverse=True)
        items = _dedupe_headlines(items)
        return items[:max_items]
    except Exception as e:
        log.info("%s: headline fetch failed: %s", symbol, e)
        return []


def _json_only(text: str) -> Dict[str, Any]:
    clean = (text or "").strip()
    clean = clean.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)


def _snapshot_fingerprint(sig: Dict[str, Any]) -> str:
    # Compact fingerprint for caching.
    ind = sig.get("indicators") or {}
    raw = {
        "a": sig.get("action"),
        "c": sig.get("confidence"),
        "p": sig.get("price"),
        "rsi": ind.get("rsi14"),
        "macdh": ind.get("macd_hist"),
        "adx": ind.get("adx14"),
        "atr": ind.get("atr14"),
        "sr": [ind.get("support20"), ind.get("resistance20")],
        "du": ind.get("breakout_up"),
        "dd": ind.get("breakout_down"),
    }
    return hashlib.md5(json.dumps(raw, sort_keys=True).encode()).hexdigest()[:12]


def call_anthropic_validate(symbol: str, snapshot: Dict[str, Any], headlines: List[Dict[str, str]], api_key: str) -> Optional[Dict[str, Any]]:
    if not api_key:
        return None

    b = _bucket()
    fp = _snapshot_fingerprint({"action": snapshot.get("action"), "confidence": snapshot.get("confidence"), "price": snapshot.get("price"), "indicators": snapshot.get("indicators")})
    key = _ckey(symbol, snapshot.get("action", ""), b, fp)

    if key in _cache:
        e = _cache[key]
        if e.get("exp") and _utc_now() < e["exp"]:
            return e["v"]

    ind = snapshot.get("indicators") or {}
    prompt_obj = {
        "symbol": symbol,
        "action": snapshot.get("action"),
        "confidence": snapshot.get("confidence"),
        "price": snapshot.get("price"),
        "indicators": {
            "rsi14": ind.get("rsi14"),
            "macd_hist": ind.get("macd_hist"),
            "adx14": ind.get("adx14"),
            "atr14": ind.get("atr14"),
            "bb_bw_pct": ind.get("bb_bw_pct"),
            "support20": ind.get("support20"),
            "resistance20": ind.get("resistance20"),
            "breakout_up": ind.get("breakout_up"),
            "breakout_down": ind.get("breakout_down"),
            "dist_to_support_pct": ind.get("dist_to_support_pct"),
            "dist_to_resistance_pct": ind.get("dist_to_resistance_pct"),
        },
        "headlines": headlines,
    }

    # Short, JSON-only response.
    system = (
        "You are a market signal validator. Use ONLY the provided snapshot and headlines. "
        "Do NOT invent prices, levels, or directions. "
        "Return strict JSON only."
    )

    user_prompt = (
        "INPUT:\n"
        + json.dumps(prompt_obj, ensure_ascii=False)
        + "\n\n"
        "OUTPUT JSON (exact keys):\n"
        "{\n"
        '"summary": "max 50 words",\n'
        '"risk_flags": ["short item"],\n'
        '"confidence_adjustment": -5..5 (integer),\n'
        '"news_bias": "bullish|bearish|neutral",\n'
        '"action_override": "none"\n'
        "}\n"
        "Rules: action_override must be \"none\" unless the provided evidence strongly contradicts the signal. "
        "If evidence is mixed set news_bias neutral and confidence_adjustment 0."
    )

    try:
        r = requests.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 260,
                "temperature": 0.1,
                "system": system,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=35,
        )
        if r.status_code != 200:
            log.info("%s: anthropic HTTP %s", symbol, r.status_code)
            return None
        text = r.json().get("content", [{}])[0].get("text", "")
        out = _json_only(text)
        # Clamp confidence adjustment to spec.
        if isinstance(out, dict) and "confidence_adjustment" in out:
            try:
                out["confidence_adjustment"] = int(out["confidence_adjustment"])
            except Exception:
                out["confidence_adjustment"] = 0
            out["confidence_adjustment"] = int(_clamp(out["confidence_adjustment"], CONFIDENCE_ADJ_MIN, CONFIDENCE_ADJ_MAX))
        _cache[key] = {"v": out, "exp": _utc_now() + timedelta(minutes=CACHE_TTL_MIN)}
        return out
    except Exception as e:
        log.info("%s: anthropic call failed: %s", symbol, e)
        return None


def _clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


def enrich_signals(signals: List[Dict[str, Any]], *, anthropic_key: str) -> List[Dict[str, Any]]:
    """
    Enrich only BUY/SELL candidates.
    AI calls are capped to MAX_LLM_ASSETS_PER_RUN.
    """
    candidates = [s for s in signals if s.get("action") in ("BUY", "SELL") and s.get("confidence", 0) > 0]
    candidates.sort(key=lambda x: abs(float(x.get("score", 0))), reverse=True)
    candidates = candidates[:MAX_LLM_ASSETS_PER_RUN]

    for sig in candidates:
        sym = sig.get("symbol")
        action = sig.get("action")
        if not sym or not action:
            continue
        # Fetch up to 3 recent headlines from yfinance.
        headlines = fetch_headlines(sym, max_items=MAX_NEWS_ITEMS_PER_ASSET)

        snapshot = {
            "action": sig.get("action"),
            "confidence": sig.get("confidence"),
            "price": sig.get("price"),
            "indicators": sig.get("indicators"),
        }
        ai = call_anthropic_validate(sym, snapshot, headlines, anthropic_key)
        if not ai:
            continue

        # Enforce "action_override must be none" and never override direction.
        if ai.get("action_override") != "none":
            ai["action_override"] = "none"

        adj = ai.get("confidence_adjustment", 0)
        try:
            adj = int(adj)
        except Exception:
            adj = 0

        sig["confidence"] = int(_clamp(int(sig.get("confidence", 0)) + adj, 0, 99))
        sig["ai_validation"] = ai
        sig["news_headlines"] = headlines

        # Keep reasons list short; don't duplicate.
        if ai.get("summary"):
            sig["reasons"] = (sig.get("reasons") or [])[:5] + ["AI summary: " + str(ai["summary"])[:80]]

    return signals

