"""
market_data.py
Fetch OHLCV from Yahoo Finance and compute a compact set of indicators.

No synthetic/fake data: missing/insufficient data returns None.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger("market_data")


def _ensure_ohlcv(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    cols = {c.lower(): c for c in df.columns}
    required = ["open", "high", "low", "close"]
    if not all(r in cols for r in required):
        return None

    o = df[cols["open"]].astype(float)
    h = df[cols["high"]].astype(float)
    l = df[cols["low"]].astype(float)
    c = df[cols["close"]].astype(float)
    if "volume" in cols:
        v = df[cols["volume"]].astype(float).fillna(0.0)
    else:
        v = pd.Series(np.zeros(len(df)), index=df.index, dtype=float)

    out = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v}, index=df.index)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def fetch_ohlcv(
    symbol: str,
    *,
    period: str = "2y",
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: str = "1d",
) -> Optional[pd.DataFrame]:
    try:
        import yfinance as yf

        t = yf.Ticker(symbol)
        df = t.history(period=period if start is None else None, start=start, end=end, interval=interval, auto_adjust=True)
        return _ensure_ohlcv(df)
    except Exception as e:
        log.warning("%s: yfinance fetch failed: %s", symbol, e)
        return None


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_g = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_l = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_g / avg_l
    return 100 - 100 / (1 + rs)


def _sma(x: pd.Series, period: int) -> pd.Series:
    return x.rolling(period).mean()


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def _stochastic(high: pd.Series, low: pd.Series, close: pd.Series, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
    ll = low.rolling(k_period).min()
    hh = high.rolling(k_period).max()
    denom = (hh - ll).replace(0, np.nan)
    k = 100 * (close - ll) / denom
    d = k.rolling(d_period).mean()
    return k, d


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    denom = (upper - lower).replace(0, np.nan)
    pos_pct = 100 * (close - lower) / denom
    bw_pct = 100 * (upper - lower) / mid.replace(0, np.nan)
    return upper, mid, lower, pos_pct, bw_pct


def _wilder_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_dm_s = pd.Series(plus_dm, index=high.index).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    minus_dm_s = pd.Series(minus_dm, index=high.index).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    plus_di = 100 * plus_dm_s / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm_s / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, plus_di, minus_di


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0.0)
    return (direction * volume).cumsum()


def _roc(close: pd.Series, period: int = 10) -> pd.Series:
    prev = close.shift(period)
    return 100 * (close / prev - 1).replace([np.inf, -np.inf], np.nan)


def _atr_percentile(atr: pd.Series, window: int = 252) -> pd.Series:
    # Percentile of last value inside rolling window.
    def pct_last(a: np.ndarray) -> float:
        if a.size < 10:
            return np.nan
        mask = ~np.isnan(a)
        if mask.sum() < 10:
            return np.nan
        arr = a[mask]
        last = arr[-1]
        return float((arr <= last).mean() * 100)

    return atr.rolling(window, min_periods=30).apply(pct_last, raw=True)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]

    out["rsi14"] = _rsi(close, 14)
    out["sma20"] = _sma(close, 20)
    out["sma50"] = _sma(close, 50)

    macd_line, macd_signal, macd_hist = _macd(close)
    out["macd_line"] = macd_line
    out["macd_signal"] = macd_signal
    out["macd_hist"] = macd_hist

    k, d = _stochastic(high, low, close)
    out["stoch_k"] = k
    out["stoch_d"] = d

    out["atr14"] = _atr(high, low, close, 14)

    upper, mid, lower, pos_pct, bw_pct = _bollinger(close, 20, 2.0)
    out["bb_upper"] = upper
    out["bb_mid"] = mid
    out["bb_lower"] = lower
    out["bb_pos_pct"] = pos_pct
    out["bb_bw_pct"] = bw_pct

    adx, plus_di, minus_di = _wilder_adx(high, low, close, 14)
    out["adx14"] = adx
    out["plus_di14"] = plus_di
    out["minus_di14"] = minus_di

    out["obv"] = _obv(close, volume)
    out["obv_slope5"] = out["obv"].diff(5)

    out["roc10"] = _roc(close, 10)

    avg20 = volume.rolling(20).mean()
    last5 = volume.rolling(5).mean()
    out["vol_ratio_pct"] = 100 * last5 / avg20.replace(0, np.nan)

    # Rolling support/resistance (close-based)
    out["support20"] = close.rolling(20).min()
    out["resistance20"] = close.rolling(20).max()
    out["dist_to_support_pct"] = 100 * (close - out["support20"]) / close.replace(0, np.nan)
    out["dist_to_resistance_pct"] = 100 * (out["resistance20"] - close) / close.replace(0, np.nan)

    # Donchian breakout levels (shifted to avoid using current bar range for threshold)
    donch_high = high.rolling(20).max()
    donch_low = low.rolling(20).min()
    out["donchian_high_prev"] = donch_high.shift(1)
    out["donchian_low_prev"] = donch_low.shift(1)
    out["breakout_up"] = close > out["donchian_high_prev"]
    out["breakout_down"] = close < out["donchian_low_prev"]

    out["atr_percentile252"] = _atr_percentile(out["atr14"], 252)

    return out


def _snapshot(symbol: str, df_ind: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df_ind is None or df_ind.empty:
        return None
    # Use the last row; if key indicators are missing, treat as insufficient real data.
    last = df_ind.iloc[-1]
    ts = df_ind.index[-1]
    key_fields = ["close", "sma20", "sma50", "atr14", "rsi14", "macd_hist", "adx14", "bb_bw_pct", "support20", "resistance20"]
    if any(pd.isna(last.get(f)) for f in key_fields):
        return None

    close = float(last["close"])
    support = float(last["support20"])
    resistance = float(last["resistance20"])
    atr = float(last["atr14"])

    return {
        "symbol": symbol,
        "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
        "last_price": round(close, 4),
        "has_real_data": True,
        "indicators": {
            "rsi14": round(float(last["rsi14"]), 3),
            "sma20": round(float(last["sma20"]), 6),
            "sma50": round(float(last["sma50"]), 6),
            "macd_hist": round(float(last["macd_hist"]), 6),
            "macd_line": round(float(last["macd_line"]), 6) if not pd.isna(last.get("macd_line")) else None,
            "macd_signal": round(float(last["macd_signal"]), 6) if not pd.isna(last.get("macd_signal")) else None,
            "stoch_k": round(float(last["stoch_k"]), 3) if not pd.isna(last.get("stoch_k")) else None,
            "stoch_d": round(float(last["stoch_d"]), 3) if not pd.isna(last.get("stoch_d")) else None,
            "atr14": round(atr, 6),
            "adx14": round(float(last["adx14"]), 3),
            "plus_di14": round(float(last["plus_di14"]), 3) if not pd.isna(last.get("plus_di14")) else None,
            "minus_di14": round(float(last["minus_di14"]), 3) if not pd.isna(last.get("minus_di14")) else None,
            "obv_slope5": round(float(last["obv_slope5"]), 3) if not pd.isna(last.get("obv_slope5")) else None,
            "roc10": round(float(last["roc10"]), 3) if not pd.isna(last.get("roc10")) else None,
            "bb_pos_pct": round(float(last["bb_pos_pct"]), 3) if not pd.isna(last.get("bb_pos_pct")) else None,
            "bb_bw_pct": round(float(last["bb_bw_pct"]), 3),
            "vol_ratio_pct": round(float(last["vol_ratio_pct"]), 3) if not pd.isna(last.get("vol_ratio_pct")) else None,
            "support20": round(support, 6),
            "resistance20": round(resistance, 6),
            "dist_to_support_pct": round(float(last["dist_to_support_pct"]), 3) if not pd.isna(last.get("dist_to_support_pct")) else None,
            "dist_to_resistance_pct": round(float(last["dist_to_resistance_pct"]), 3) if not pd.isna(last.get("dist_to_resistance_pct")) else None,
            "breakout_up": bool(last.get("breakout_up")) if not pd.isna(last.get("breakout_up")) else None,
            "breakout_down": bool(last.get("breakout_down")) if not pd.isna(last.get("breakout_down")) else None,
            "atr_percentile252": round(float(last["atr_percentile252"]), 3) if not pd.isna(last.get("atr_percentile252")) else None,
        },
        "source": "yfinance_real",
        "data_points": int(len(df_ind)),
    }


def fetch_snapshot(symbol: str, *, period: str = "2y", interval: str = "1d") -> Optional[Dict[str, Any]]:
    df = fetch_ohlcv(symbol, period=period, interval=interval)
    if df is None:
        return None
    df_ind = compute_indicators(df)
    if len(df_ind) < 60:
        return None
    return _snapshot(symbol, df_ind)


def fetch_all_snapshots(
    symbols: list[str], *, period: str = "2y", interval: str = "1d"
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for s in symbols:
        snap = fetch_snapshot(s, period=period, interval=interval)
        if snap is not None:
            out[s] = snap
        else:
            log.warning("%s: NO_DATA (insufficient indicators)", s)
    return out


def fetch_indicator_frame(
    symbol: str,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    period: str = "3y",
    interval: str = "1d",
) -> Optional[pd.DataFrame]:
    df = fetch_ohlcv(symbol, start=start, end=end, period=period, interval=interval)
    if df is None or len(df) < 80:
        return None
    return compute_indicators(df)

