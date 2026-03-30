"""
signal_engine.py
Pure quantitative logic only.

build_quant_signal(row, asset_meta) -> structured signal dict
No AI influence. No randomness. Entry/SL/TP computed from real indicator levels.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

Action = str  # "BUY"|"SELL"|"HOLD"|"WATCHLIST"|"NO_DATA"

SCORE_TRADE_THRESHOLD = int(os.getenv("SCORE_THRESHOLD", "35"))
ADX_MIN = float(os.getenv("ADX_MIN", "20"))
VOL_RATIO_MIN = float(os.getenv("VOL_RATIO_MIN", "105"))  # volume ratio % vs baseline
DIST_MIN_PCT = float(os.getenv("DIST_MIN_PCT", "0.25"))  # distance to S/R in %

RSI_BUY_MIN = float(os.getenv("RSI_BUY_MIN", "45"))
RSI_BUY_MAX = float(os.getenv("RSI_BUY_MAX", "70"))
RSI_SELL_MIN = float(os.getenv("RSI_SELL_MIN", "30"))
RSI_SELL_MAX = float(os.getenv("RSI_SELL_MAX", "60"))


def _clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


def _confidence_from_score(score_norm: int) -> int:
    # score_norm is ~[-100..100] -> confidence in [1..99]
    return int(round(_clamp(abs(score_norm) * 0.85 + 8, 1, 99)))


def _compute_trade_levels_buy(price: float, support: float, resistance: float, atr: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    entry = price
    sl = support - 0.25 * atr
    tp = resistance + 0.15 * atr
    # Fallback if S/R are inverted or too tight.
    if not (sl < entry < tp):
        sl = price - 0.9 * atr
        tp = price + 1.2 * atr
    if not (sl < entry < tp):
        return None, None, None
    return entry, sl, tp


def _compute_trade_levels_sell(price: float, support: float, resistance: float, atr: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    entry = price
    sl = resistance + 0.25 * atr
    tp = support - 0.15 * atr
    # Fallback if S/R are inverted or too tight.
    if not (tp < entry < sl):
        sl = price + 0.9 * atr
        tp = price - 1.2 * atr
    if not (tp < entry < sl):
        return None, None, None
    return entry, sl, tp


def build_quant_signal(row: Union[pd.Series, Dict[str, Any]], asset_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure function:
      - input: indicator values at time t
      - output: BUY/SELL/HOLD/WATCHLIST/NO_DATA with entry/SL/TP levels (quantitative only)

    Input variants:
      - market_data snapshot dict:
          {"last_price":..., "timestamp":..., "has_real_data":true, "indicators":{...}}
      - backtest indicator frame row (pd.Series):
          columns like close, sma20, sma50, atr14, rsi14, etc.
    """
    symbol = asset_meta.get("symbol", "")
    name = asset_meta.get("name", "")
    market = asset_meta.get("market", "")
    asset_type = asset_meta.get("asset_type", "")

    if isinstance(row, dict) and "indicators" in row:
        price = float(row["last_price"])
        ind = row["indicators"]
        ts = row.get("timestamp")
        has_real_data = bool(row.get("has_real_data", True))
    else:
        series = row if isinstance(row, pd.Series) else pd.Series(row)
        price = float(series.get("close", series.get("last_price", np.nan)))
        ind = dict(series)
        ts = getattr(series, "name", None)
        has_real_data = True

    def g(k: str) -> float:
        v = ind.get(k, np.nan)
        try:
            return float(v)
        except Exception:
            return float("nan")

    # Required indicators for robust decision + SL/TP.
    rsi = g("rsi14")
    sma20 = g("sma20")
    sma50 = g("sma50")
    macd_hist = g("macd_hist")
    adx14 = g("adx14")
    plus_di = g("plus_di14")
    minus_di = g("minus_di14")
    atr14 = g("atr14")
    bb_bw_pct = g("bb_bw_pct")
    support20 = g("support20")
    resistance20 = g("resistance20")
    dist_to_support_pct = g("dist_to_support_pct")
    dist_to_resistance_pct = g("dist_to_resistance_pct")

    breakout_up = bool(ind.get("breakout_up")) if not pd.isna(ind.get("breakout_up", np.nan)) else False
    breakout_down = bool(ind.get("breakout_down")) if not pd.isna(ind.get("breakout_down", np.nan)) else False

    vol_ratio_pct = g("vol_ratio_pct")
    roc10 = g("roc10")
    obv_slope5 = g("obv_slope5")

    atr_pct = ind.get("atr_percentile252", None)
    try:
        atr_percentile252 = float(atr_pct) if atr_pct is not None and not pd.isna(atr_pct) else None
    except Exception:
        atr_percentile252 = None

    key_ok = not any(pd.isna(x) for x in [price, rsi, sma20, sma50, atr14, macd_hist, adx14, bb_bw_pct, support20, resistance20])
    if not key_ok:
        return {
            "symbol": symbol,
            "name": name,
            "market": market,
            "asset_type": asset_type,
            "action": "NO_DATA",
            "confidence": 0,
            "score_breakdown": [],
            "reasons": ["insufficient indicators for robust signal"],
            "price": None,
            "indicators": {"missing": True},
            "entry": None,
            "stop_loss": None,
            "take_profit": None,
            "risk_reward": None,
            "has_real_data": has_real_data,
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else (ts or None),
            "score": 0,
        }

    # Directional features
    trend_bull = price > sma20 > sma50
    trend_bear = price < sma20 < sma50

    macd_bull = macd_hist > 0
    macd_bear = macd_hist < 0

    rsi_bull = RSI_BUY_MIN <= rsi <= RSI_BUY_MAX
    rsi_bear = RSI_SELL_MIN <= rsi <= RSI_SELL_MAX

    adx_strong = adx14 >= ADX_MIN
    di_bull = plus_di > minus_di
    di_bear = minus_di > plus_di

    vol_ok = (vol_ratio_pct >= VOL_RATIO_MIN) if not pd.isna(vol_ratio_pct) else False

    # Distance gating to avoid chasing extremes.
    dist_res_ok = (not pd.isna(dist_to_resistance_pct)) and dist_to_resistance_pct >= DIST_MIN_PCT
    dist_sup_ok = (not pd.isna(dist_to_support_pct)) and dist_to_support_pct >= DIST_MIN_PCT

    squeeze = (bb_bw_pct < 10) or (atr_percentile252 is not None and atr_percentile252 < 30)

    score = 0.0
    breakdown: List[Dict[str, Any]] = []
    reasons: List[str] = []

    def add(factor: str, contrib: float, reason: Optional[str] = None) -> None:
        nonlocal score
        if contrib == 0:
            return
        score += contrib
        breakdown.append({"factor": factor, "contribution": int(round(contrib))})
        if reason:
            reasons.append(reason)

    if trend_bull:
        add("trend_ma", 25, "close > SMA20 > SMA50")
    elif trend_bear:
        add("trend_ma", -25, "close < SMA20 < SMA50")

    if macd_bull:
        add("macd_hist", 20, "MACD histogram > 0")
    elif macd_bear:
        add("macd_hist", -20, "MACD histogram < 0")

    if rsi_bull and trend_bull:
        add("rsi_zone", 10, "RSI in constructive bull range")
    elif rsi_bear and trend_bear:
        add("rsi_zone", -10, "RSI in constructive bear range")

    if adx_strong and di_bull:
        add("adx_di", 15, "ADX strong and +DI > -DI")
    elif adx_strong and di_bear:
        add("adx_di", -15, "ADX strong and -DI > +DI")

    if vol_ok and (trend_bull or breakout_up):
        add("volume", 8, "volume ratio supports bullish move")
    elif vol_ok and (trend_bear or breakout_down):
        add("volume", -8, "volume ratio supports bearish move")

    if breakout_up:
        add("donchian_breakout", 10, "breakout above rolling highs")
    elif breakout_down:
        add("donchian_breakout", -10, "breakdown below rolling lows")

    if not pd.isna(roc10) and roc10 > 0:
        add("roc", 5, "ROC positive")
    elif not pd.isna(roc10) and roc10 < 0:
        add("roc", -5, "ROC negative")

    if not pd.isna(obv_slope5) and obv_slope5 > 0:
        add("obv", 4, "OBV slope positive")
    elif not pd.isna(obv_slope5) and obv_slope5 < 0:
        add("obv", -4, "OBV slope negative")

    if trend_bull and dist_res_ok:
        add("distance_res", 4, "enough distance to resistance")
    if trend_bear and dist_sup_ok:
        add("distance_sup", -4, "enough distance to support")

    if not squeeze and breakout_up and trend_bull:
        add("vol_regime", 3, "not in squeeze; breakout more likely to extend")
    elif not squeeze and breakout_down and trend_bear:
        add("vol_regime", -3, "not in squeeze; breakdown more likely to extend")

    score_norm = int(round(_clamp(score, -100, 100)))
    confidence = _confidence_from_score(score_norm)

    # Action rules (direction is purely quant).
    action: Action = "HOLD"
    if score_norm >= SCORE_TRADE_THRESHOLD and trend_bull and macd_bull and (not adx_strong or di_bull):
        action = "BUY"
    elif score_norm <= -SCORE_TRADE_THRESHOLD and trend_bear and macd_bear and (not adx_strong or di_bear):
        action = "SELL"
    else:
        near = abs(score_norm) >= max(15, int(SCORE_TRADE_THRESHOLD * 0.65))
        if near and (breakout_up or breakout_down or squeeze):
            action = "WATCHLIST"

    entry = sl = tp = None
    risk_reward = None

    if action in ("BUY", "SELL", "WATCHLIST"):
        if resistance20 <= support20 or pd.isna(atr14):
            action = "NO_DATA"
            confidence = 0
        else:
            if action == "BUY":
                entry, sl, tp = _compute_trade_levels_buy(price, support20, resistance20, atr14)
            elif action == "SELL":
                entry, sl, tp = _compute_trade_levels_sell(price, support20, resistance20, atr14)
            else:
                # WATCHLIST: pick direction implied by score sign.
                if score_norm >= 0:
                    entry, sl, tp = _compute_trade_levels_buy(price, support20, resistance20, atr14)
                    action = "WATCHLIST" if entry is not None else "NO_DATA"
                else:
                    entry, sl, tp = _compute_trade_levels_sell(price, support20, resistance20, atr14)
                    action = "WATCHLIST" if entry is not None else "NO_DATA"

            if entry is None or sl is None or tp is None:
                action = "NO_DATA"
                confidence = 0

            if action != "NO_DATA" and abs(entry - sl) > 0:
                risk_reward = round(abs(tp - entry) / abs(entry - sl), 3)

    if action in ("HOLD", "NO_DATA"):
        entry = sl = tp = None
        risk_reward = None

    indicators_snapshot = {
        "rsi14": round(rsi, 3),
        "sma20": round(sma20, 6),
        "sma50": round(sma50, 6),
        "macd_hist": round(macd_hist, 6),
        "adx14": round(adx14, 3),
        "plus_di14": round(plus_di, 3) if not pd.isna(plus_di) else None,
        "minus_di14": round(minus_di, 3) if not pd.isna(minus_di) else None,
        "atr14": round(atr14, 6),
        "bb_bw_pct": round(bb_bw_pct, 3),
        "support20": round(support20, 6),
        "resistance20": round(resistance20, 6),
        "dist_to_support_pct": round(dist_to_support_pct, 3) if not pd.isna(dist_to_support_pct) else None,
        "dist_to_resistance_pct": round(dist_to_resistance_pct, 3) if not pd.isna(dist_to_resistance_pct) else None,
        "breakout_up": bool(breakout_up),
        "breakout_down": bool(breakout_down),
        "atr_percentile252": round(atr_percentile252, 3) if atr_percentile252 is not None else None,
        "vol_ratio_pct": round(vol_ratio_pct, 3) if not pd.isna(vol_ratio_pct) else None,
        "roc10": round(roc10, 3) if not pd.isna(roc10) else None,
        "obv_slope5": round(obv_slope5, 3) if not pd.isna(obv_slope5) else None,
    }

    ts_out = ts.isoformat() if hasattr(ts, "isoformat") else (ts or None)
    return {
        "symbol": symbol,
        "name": name,
        "market": market,
        "asset_type": asset_type,
        "action": action,
        "confidence": int(confidence),
        "score_breakdown": breakdown,
        "reasons": reasons[:6],
        "price": round(price, 6),
        "indicators": indicators_snapshot,
        "entry": round(entry, 6) if entry is not None else None,
        "stop_loss": round(sl, 6) if sl is not None else None,
        "take_profit": round(tp, 6) if tp is not None else None,
        "risk_reward": risk_reward,
        "has_real_data": bool(has_real_data),
        "timestamp": ts_out,
        "score": score_norm,
    }

