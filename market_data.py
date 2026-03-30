# market_data.py - Gold Intelligence v1.4.0
# Dati reali via yfinance + indicatori tecnici completi
# Indicatori: RSI, Bollinger Bands, MA(20/50/200), MACD, Volume, ATR, Stocastico

import logging
import warnings
from datetime import datetime
from typing   import Dict, Any, Optional

import numpy  as np
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger("market_data")


# ── INDICATORI TECNICI ────────────────────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = 14) -> float:
    delta  = close.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_g  = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_l  = loss.ewm(com=period - 1, min_periods=period).mean()
    rs     = avg_g / avg_l
    rsi    = 100 - 100 / (1 + rs)
    return round(float(rsi.iloc[-1]), 1)


def calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> Dict:
    mid   = close.rolling(period).mean()
    std   = close.rolling(period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    last  = close.iloc[-1]
    ul    = float(upper.iloc[-1])
    ll    = float(lower.iloc[-1])
    ml    = float(mid.iloc[-1])
    bw    = round((ul - ll) / ml * 100, 2) if ml else 0   # bandwidth %
    pos   = round((last - ll) / (ul - ll) * 100, 1) if (ul - ll) else 50  # % position
    signal = "IPERCOMPRATO" if pos > 80 else "IPERVENDUTO" if pos < 20 else "NEUTRALE"
    return {
        "upper":     round(ul, 2),
        "middle":    round(ml, 2),
        "lower":     round(ll, 2),
        "position":  pos,   # 0=lower 100=upper
        "bandwidth": bw,
        "signal":    signal,
    }


def calc_ma_signals(close: pd.Series) -> Dict:
    ma20  = close.rolling(20).mean()
    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()
    last  = close.iloc[-1]

    v20  = float(ma20.iloc[-1])
    v50  = float(ma50.iloc[-1])  if len(close) >= 50  else None
    v200 = float(ma200.iloc[-1]) if len(close) >= 200 else None

    # Crossover detection (oggi vs ieri)
    cross_signal = "nessuno"
    if v50 and len(close) >= 51:
        prev_20  = float(ma20.iloc[-2])
        prev_50  = float(ma50.iloc[-2])
        if prev_20 < prev_50 and v20 > v50:
            cross_signal = "GOLDEN CROSS (20 supera 50) - segnale rialzista"
        elif prev_20 > prev_50 and v20 < v50:
            cross_signal = "DEATH CROSS (20 sotto 50) - segnale ribassista"
        elif v20 > v50:
            cross_signal = "MA20 sopra MA50 - trend rialzista"
        else:
            cross_signal = "MA20 sotto MA50 - trend ribassista"

    # Pendenza MA20 (momentum)
    slope_ma20 = None
    if len(ma20.dropna()) >= 5:
        vals  = ma20.dropna().iloc[-5:]
        slope = (float(vals.iloc[-1]) - float(vals.iloc[0])) / float(vals.iloc[0]) * 100
        slope_ma20 = round(slope, 3)

    return {
        "ma20":         round(v20, 2) if v20 else None,
        "ma50":         round(v50, 2) if v50 else None,
        "ma200":        round(v200, 2) if v200 else None,
        "price_vs_ma20": round((last - v20) / v20 * 100, 2) if v20 else None,
        "price_vs_ma50": round((last - v50) / v50 * 100, 2) if v50 else None,
        "price_vs_ma200":round((last - v200) / v200 * 100, 2) if v200 else None,
        "cross_signal":  cross_signal,
        "slope_ma20_5d": slope_ma20,
    }


def calc_macd(close: pd.Series) -> Dict:
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist   = macd - signal
    return {
        "macd":      round(float(macd.iloc[-1]), 4),
        "signal":    round(float(signal.iloc[-1]), 4),
        "histogram": round(float(hist.iloc[-1]), 4),
        "trend":     "rialzista" if float(hist.iloc[-1]) > 0 else "ribassista",
        "crossing":  (
            "BULLISH CROSSOVER" if float(hist.iloc[-2]) < 0 < float(hist.iloc[-1]) else
            "BEARISH CROSSOVER" if float(hist.iloc[-2]) > 0 > float(hist.iloc[-1]) else
            "nessuno"
        ) if len(hist) >= 2 else "n/a",
    }


def calc_stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                    k_period: int = 14, d_period: int = 3) -> Dict:
    low_k  = low.rolling(k_period).min()
    high_k = high.rolling(k_period).max()
    k = 100 * (close - low_k) / (high_k - low_k)
    d = k.rolling(d_period).mean()
    kv = round(float(k.iloc[-1]), 1)
    dv = round(float(d.iloc[-1]), 1)
    signal = "IPERCOMPRATO" if kv > 80 else "IPERVENDUTO" if kv < 20 else "NEUTRALE"
    return {"k": kv, "d": dv, "signal": signal}


def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return round(float(tr.rolling(period).mean().iloc[-1]), 4)


def calc_volume_trend(volume: pd.Series) -> Dict:
    avg20 = float(volume.rolling(20).mean().iloc[-1])
    last5 = float(volume.iloc[-5:].mean())
    ratio = round(last5 / avg20 * 100, 1) if avg20 else 100
    return {
        "avg20":      int(avg20),
        "last5_avg":  int(last5),
        "ratio_pct":  ratio,
        "signal":     "ALTO" if ratio > 120 else "BASSO" if ratio < 80 else "NORMALE",
    }


def calc_support_resistance(close: pd.Series, window: int = 20) -> Dict:
    recent = close.iloc[-window:]
    return {
        "support":    round(float(recent.min()), 2),
        "resistance": round(float(recent.max()), 2),
        "range_pct":  round((float(recent.max()) - float(recent.min())) / float(recent.min()) * 100, 2),
    }


def calc_performance(close: pd.Series) -> Dict:
    last = float(close.iloc[-1])
    def pct(n):
        if len(close) < n + 1:
            return None
        return round((last - float(close.iloc[-(n+1)])) / float(close.iloc[-(n+1)]) * 100, 2)
    return {
        "1d": pct(1), "5d": pct(5), "20d": pct(20),
        "60d": pct(60) if len(close) >= 61 else None,
    }


# ── FETCH DATI REALI ──────────────────────────────────────────────────────────

def fetch_technicals(symbol: str, period: str = "90d") -> Optional[Dict]:
    """
    Scarica dati reali da Yahoo Finance e calcola tutti gli indicatori.
    Ritorna None in caso di errore (fallback a dati simulati).
    """
    try:
        import yfinance as yf
        t   = yf.Ticker(symbol)
        df  = t.history(period=period, interval="1d", auto_adjust=True)
        if df is None or len(df) < 30:
            log.warning(symbol + ": dati insufficienti (" + str(len(df) if df is not None else 0) + " giorni)")
            return None

        close  = df["Close"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]
        last   = round(float(close.iloc[-1]), 2)
        prev   = round(float(close.iloc[-2]), 2)

        indicators = {
            "symbol":      symbol,
            "last_price":  last,
            "prev_close":  prev,
            "change_pct":  round((last - prev) / prev * 100, 2),
            "data_points": len(close),
            "last_date":   str(close.index[-1].date()),
            "rsi":         calc_rsi(close),
            "bollinger":   calc_bollinger(close),
            "ma":          calc_ma_signals(close),
            "macd":        calc_macd(close),
            "stochastic":  calc_stochastic(high, low, close),
            "atr":         calc_atr(high, low, close),
            "volume":      calc_volume_trend(volume),
            "support_res": calc_support_resistance(close),
            "performance": calc_performance(close),
            "source":      "yfinance_real",
        }

        log.info(symbol + " fetch OK: price=" + str(last)
                 + " RSI=" + str(indicators["rsi"])
                 + " BB=" + str(indicators["bollinger"]["position"]) + "%"
                 + " cross=" + indicators["ma"]["cross_signal"][:20])
        return indicators

    except ImportError:
        log.error("yfinance non installato")
        return None
    except Exception as e:
        log.warning(symbol + " fetch fallito: " + str(e))
        return None


def fetch_all(symbols: list) -> Dict[str, Dict]:
    """Scarica indicatori per tutti i simboli. Fallback silenzioso per ogni errore."""
    results = {}
    for sym in symbols:
        data = fetch_technicals(sym)
        if data:
            results[sym] = data
        else:
            log.warning(sym + ": usando dati tecnici non disponibili")
    return results
