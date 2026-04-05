# fundamental_layer.py v1.0
# Layer fondamentali + istituzionali via Financial Modeling Prep (FMP)
# Frequenza: quarterly per fundamentals, daily per institutional changes
# Output: fundamental_score (-15 a +15) per asset

import os, json, logging, time
from typing import Dict, List, Optional, Tuple
import requests

log = logging.getLogger("fundamental")

_FMP_BASE  = "https://financialmodelingprep.com/api/v3"
_CACHE: Dict[str, Dict] = {}
CACHE_TTL  = 12 * 3600  # 12 ore (dati trimestrali cambiano lentamente)

_SESSION: Optional[requests.Session] = None

def _sess() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({"Accept": "application/json"})
        _SESSION.proxies = {"http": None, "https": None}
        for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]:
            os.environ.pop(k, None)
    return _SESSION


def _fmp_get(endpoint: str, fmp_key: str, params: Dict = None) -> Optional[Dict]:
    if not fmp_key:
        return None
    try:
        p = {"apikey": fmp_key}
        if params:
            p.update(params)
        r = _sess().get(f"{_FMP_BASE}/{endpoint}", params=p, timeout=15)
        if r.status_code == 200:
            return r.json()
        log.warning(f"[FUND] FMP {endpoint}: HTTP {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"[FUND] FMP {endpoint}: {e}")
        return None


# ── Fundamentals per azioni ───────────────────────────────────────────────────
def _get_ratios(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Key Metrics TTM: PE, EPS growth, revenue growth, FCF, margini."""
    data = _fmp_get(f"key-metrics-ttm/{symbol}", fmp_key)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None

def _get_income(symbol: str, fmp_key: str, limit: int = 4) -> Optional[List]:
    """Income statement ultimi N trimestri per calcolare trend."""
    return _fmp_get(f"income-statement/{symbol}", fmp_key, {"period": "quarter", "limit": str(limit)})

def _get_institutional(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Institutional holders e variazioni."""
    holders = _fmp_get(f"institutional-holder/{symbol}", fmp_key)
    return holders if holders and isinstance(holders, list) else None

def _get_insider(symbol: str, fmp_key: str) -> Optional[List]:
    """Insider trading ultimi 90 giorni."""
    return _fmp_get(f"insider-trading", fmp_key, {"symbol": symbol, "limit": "20"})

def _get_etf_info(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Info ETF: AUM, expense ratio, net assets."""
    data = _fmp_get(f"etf-info/{symbol}", fmp_key)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


# ── Scoring fondamentali per azioni ──────────────────────────────────────────

def _estimate_fair_value(metrics: Dict, price: float) -> Dict:
    """
    Stima Fair Value con 3 modelli semplificati:
    1. Graham Number: sqrt(22.5 * EPS * BookValue)
    2. PE Forward: EPS_next * settore_PE_medio
    3. FCF Yield: FCF / shares * 20 (multiplo conservativo)
    Output: fair_value_avg, upside_pct, valuation (cheap/fair/expensive)
    """
    if not metrics or not price or price <= 0:
        return {"fair_value": None, "upside_pct": None, "valuation": "unknown"}

    estimates = []

    # Modello 1: Graham Number
    eps_ttm = metrics.get("netIncomePerShareTTM") or metrics.get("epsTTM")
    bv      = metrics.get("bookValuePerShareTTM")
    if eps_ttm and bv and eps_ttm > 0 and bv > 0:
        graham = round((22.5 * eps_ttm * bv) ** 0.5, 2)
        estimates.append(graham)

    # Modello 2: PE Ratio medio settore (proxy: usa PE forward con PE = 15x conservative)
    pe_fwd = metrics.get("peRatioTTM")
    if pe_fwd and 0 < pe_fwd < 80 and eps_ttm and eps_ttm > 0:
        pe_target = min(pe_fwd, 20)  # cap a 20x per essere conservativi
        pe_fv = round(eps_ttm * pe_target, 2)
        estimates.append(pe_fv)

    # Modello 3: FCF Yield
    fcf_per_share = metrics.get("freeCashFlowPerShareTTM")
    if fcf_per_share and fcf_per_share > 0:
        fcf_fv = round(fcf_per_share * 18, 2)  # 18x FCF = ~5.5% yield
        estimates.append(fcf_fv)

    if not estimates:
        return {"fair_value": None, "upside_pct": None, "valuation": "no_data"}

    fv_avg = round(sum(estimates) / len(estimates), 2)
    upside = round((fv_avg - price) / price * 100, 1)

    if upside > 25:   valuation = "molto_sottovalutato"
    elif upside > 10: valuation = "sottovalutato"
    elif upside > -10:valuation = "fair_value"
    elif upside > -25:valuation = "sopravvalutato"
    else:             valuation = "molto_sopravvalutato"

    return {
        "fair_value":   fv_avg,
        "upside_pct":   upside,
        "valuation":    valuation,
        "models_used":  len(estimates),
        "estimates":    {"graham": estimates[0] if len(estimates)>0 else None,
                         "pe_fwd": estimates[1] if len(estimates)>1 else None,
                         "fcf":    estimates[2] if len(estimates)>2 else None},
    }


def _financial_health_score(metrics: Dict, income_data: list) -> Dict:
    """
    Financial Health Score 1-5 (5 = ottima salute finanziaria).
    Criteri:
    - D/E ratio (debt/equity)
    - Free Cash Flow positivo e crescente
    - Revenue growth > 5%
    - Margine operativo > 10%
    - Current ratio > 1.5
    """
    score = 0
    detail = {}

    # 1. Debt/Equity (max 1 pt)
    de = metrics.get("debtEquityRatioTTM")
    if de is not None:
        if de < 0.3:   score += 1.0; detail["debt"] = "basso"
        elif de < 1.0: score += 0.7; detail["debt"] = "moderato"
        elif de < 2.0: score += 0.3; detail["debt"] = "alto"
        else:          score += 0;   detail["debt"] = "critico"
    
    # 2. Free Cash Flow (max 1 pt)
    fcf = metrics.get("freeCashFlowPerShareTTM")
    if fcf is not None:
        if fcf > 5:    score += 1.0; detail["fcf"] = "eccellente"
        elif fcf > 0:  score += 0.7; detail["fcf"] = "positivo"
        else:          score += 0;   detail["fcf"] = "negativo"

    # 3. Revenue growth (max 1 pt)
    rev_g = metrics.get("revenueGrowth") or metrics.get("revenueGrowthTTM")
    if rev_g is not None:
        if rev_g > 0.15:  score += 1.0; detail["rev_growth"] = f"+{rev_g*100:.0f}%"
        elif rev_g > 0.05:score += 0.7; detail["rev_growth"] = f"+{rev_g*100:.0f}%"
        elif rev_g > 0:   score += 0.3; detail["rev_growth"] = f"+{rev_g*100:.0f}%"
        else:             score += 0;   detail["rev_growth"] = f"{rev_g*100:.0f}%"

    # 4. Margine operativo (max 1 pt)
    margin = metrics.get("operatingProfitMarginTTM")
    if margin is not None:
        if margin > 0.20:  score += 1.0; detail["margin"] = f"{margin*100:.0f}%"
        elif margin > 0.10:score += 0.7; detail["margin"] = f"{margin*100:.0f}%"
        elif margin > 0:   score += 0.3; detail["margin"] = f"{margin*100:.0f}%"
        else:              score += 0;   detail["margin"] = "negativo"

    # 5. Current ratio (max 1 pt)
    cr = metrics.get("currentRatioTTM")
    if cr is not None:
        if cr > 2.0:   score += 1.0; detail["liquidity"] = "ottima"
        elif cr > 1.5: score += 0.7; detail["liquidity"] = "buona"
        elif cr > 1.0: score += 0.3; detail["liquidity"] = "sufficiente"
        else:          score += 0;   detail["liquidity"] = "critica"

    health_1_5 = max(1.0, min(5.0, round(1 + score * 4 / 5, 1)))
    label = ("Eccellente" if health_1_5 >= 4.5 else
             "Buona"      if health_1_5 >= 3.5 else
             "Discreta"   if health_1_5 >= 2.5 else
             "Debole"     if health_1_5 >= 1.5 else "Critica")

    return {"score_1_5": health_1_5, "label": label, "detail": detail}


def _score_stock(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    fundamental_score per azione: -15 a +15
    Dimensioni: qualità, crescita, valutazione, flussi di cassa
    """
    score     = 0
    detail    = {}
    reasons   = []

    # Key metrics TTM
    metrics = _get_ratios(symbol, fmp_key)
    if not metrics:
        log.info(f"[FUND] {symbol}: no FMP data — score=0")
        return 0, {"score": 0, "detail": {}, "reasons": ["Fondamentali non disponibili"], "source": "none"}
    detail["_metrics_raw"] = metrics  # conservato per Fair Value con prezzo reale

    # 1. Revenue growth QoQ (+/-4)
    rev_growth = metrics.get("revenueGrowth")
    if rev_growth is not None:
        if rev_growth > 0.20:
            score += 4; detail["rev_growth"] = +4; reasons.append(f"Revenue +{rev_growth*100:.1f}% → crescita forte")
        elif rev_growth > 0.05:
            score += 2; detail["rev_growth"] = +2; reasons.append(f"Revenue +{rev_growth*100:.1f}% → crescita sana")
        elif rev_growth > 0:
            score += 1; detail["rev_growth"] = +1
        elif rev_growth > -0.10:
            score -= 1; detail["rev_growth"] = -1
        else:
            score -= 4; detail["rev_growth"] = -4; reasons.append(f"Revenue {rev_growth*100:.1f}% → contrazione")

    # 2. Margine operativo (+/-3)
    op_margin = metrics.get("operatingIncomeRatioTTM") or metrics.get("operatingProfitMargin")
    if op_margin is not None:
        if op_margin > 0.20:
            score += 3; detail["op_margin"] = +3; reasons.append(f"Margine operativo {op_margin*100:.1f}% → eccellente")
        elif op_margin > 0.10:
            score += 1; detail["op_margin"] = +1; reasons.append(f"Margine operativo {op_margin*100:.1f}% → sano")
        elif op_margin > 0:
            pass
        else:
            score -= 3; detail["op_margin"] = -3; reasons.append(f"Margine operativo negativo → rischio")

    # 3. Free Cash Flow Yield (+/-3)
    fcf_ps  = metrics.get("freeCashFlowPerShareTTM")
    price   = metrics.get("marketCapTTM")
    if fcf_ps and price and price > 0:
        fcf_yield = fcf_ps / (price / (metrics.get("sharesOutstanding") or 1))
        if fcf_yield is not None and abs(fcf_yield) < 100:  # sanity check
            if fcf_yield > 0.08:
                score += 3; detail["fcf_yield"] = +3; reasons.append(f"FCF Yield {fcf_yield*100:.1f}% → ottimo")
            elif fcf_yield > 0.04:
                score += 1; detail["fcf_yield"] = +1
            elif fcf_yield < 0:
                score -= 2; detail["fcf_yield"] = -2; reasons.append("FCF negativo → attenzione")

    # 4. Valutazione P/E (+/-3)
    pe = metrics.get("peRatioTTM")
    if pe is not None and pe > 0:
        if pe < 15:
            score += 3; detail["pe"] = +3; reasons.append(f"P/E {pe:.1f} → sottovalutato")
        elif pe < 25:
            score += 1; detail["pe"] = +1; reasons.append(f"P/E {pe:.1f} → valutazione ragionevole")
        elif pe < 40:
            score -= 1; detail["pe"] = -1
        else:
            score -= 3; detail["pe"] = -3; reasons.append(f"P/E {pe:.1f} → sopravvalutato")
    elif pe is not None and pe < 0:
        score -= 2; detail["pe"] = -2; reasons.append("P/E negativo (perdite)")

    # 5. Debt/Equity (+/-2)
    de = metrics.get("debtToEquityTTM")
    if de is not None:
        if de < 0.3:
            score += 2; detail["debt"] = +2; reasons.append("Basso indebitamento")
        elif de < 1.0:
            score += 0
        elif de < 2.0:
            score -= 1; detail["debt"] = -1
        else:
            score -= 2; detail["debt"] = -2; reasons.append(f"Debito elevato D/E={de:.1f}")

    score = max(-15, min(15, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "metrics": {
            "pe":         pe,
            "rev_growth": rev_growth,
            "op_margin":  op_margin,
            "de":         de,
        },
        "source": "FMP",
    }


# ── Scoring fondamentali per ETF ──────────────────────────────────────────────
def _score_etf(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    fundamental_score per ETF: -10 a +10
    AUM, TER, fund flows
    """
    etf = _get_etf_info(symbol, fmp_key)
    if not etf:
        return 0, {"score": 0, "detail": {}, "reasons": ["ETF info non disponibili"], "source": "none"}

    score   = 0
    detail  = {}
    reasons = []

    aum = etf.get("netAssets") or etf.get("totalAssets")
    ter = etf.get("expenseRatio")

    # AUM (+/-3)
    if aum:
        if aum > 10e9:
            score += 3; detail["aum"] = +3; reasons.append(f"AUM {aum/1e9:.1f}B → ETF liquido e stabile")
        elif aum > 1e9:
            score += 1; detail["aum"] = +1
        elif aum < 100e6:
            score -= 2; detail["aum"] = -2; reasons.append("AUM basso → rischio liquidità")

    # TER (+/-2)
    if ter is not None:
        if ter < 0.10:
            score += 2; detail["ter"] = +2; reasons.append(f"TER {ter:.2f}% → ottimo costo")
        elif ter < 0.30:
            score += 1; detail["ter"] = +1
        elif ter > 0.60:
            score -= 2; detail["ter"] = -2; reasons.append(f"TER {ter:.2f}% → costoso")

    score = max(-10, min(10, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "metrics": {"aum": aum, "ter": ter},
        "source":  "FMP",
    }


# ── Institutional score ────────────────────────────────────────────────────────
def _institutional_score(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    institutional_score: -10 a +10
    Basato su: concentrazione holder, trend insider buying, ownership change
    """
    score   = 0
    detail  = {}
    reasons = []

    # Institutional holders
    holders = _get_institutional(symbol, fmp_key)
    if holders:
        n_holders = len(holders)
        total_shares = sum(h.get("shares", 0) for h in holders)
        # Concentrazione top-5
        top5 = sorted(holders, key=lambda h: h.get("shares", 0), reverse=True)[:5]
        top5_shares = sum(h.get("shares", 0) for h in top5)
        concentration = top5_shares / total_shares if total_shares > 0 else 0

        if n_holders > 500:
            score += 2; detail["n_holders"] = +2; reasons.append(f"{n_holders} istituzioni → ampia distribuzione")
        elif n_holders > 100:
            score += 1; detail["n_holders"] = +1
        elif n_holders < 20:
            score -= 1; detail["n_holders"] = -1; reasons.append("Pochi holder istituzionali")

        if concentration < 0.20:
            score += 2; detail["concentration"] = +2; reasons.append("Ownership distribuita → minor rischio crowding")
        elif concentration > 0.50:
            score -= 1; detail["concentration"] = -1; reasons.append("Alta concentrazione → crowding risk")

    # Insider trading
    insider = _get_insider(symbol, fmp_key)
    if insider:
        buys  = [t for t in insider if t.get("transactionType", "").upper() in ("P-PURCHASE","BUY","B")]
        sells = [t for t in insider if t.get("transactionType", "").upper() in ("S-SALE","SELL","S")]
        net_trades = len(buys) - len(sells)
        if net_trades >= 2:
            score += 3; detail["insider"] = +3; reasons.append(f"Insider buying ({len(buys)} acquisti) → segnale forte")
        elif net_trades >= 1:
            score += 2; detail["insider"] = +2; reasons.append("Insider buying recente")
        elif net_trades <= -2:
            score -= 2; detail["insider"] = -2; reasons.append(f"Insider selling ({len(sells)} vendite)")

    score = max(-10, min(10, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "n_holders": len(holders) if holders else None,
        "insider_net": len(buys) - len(sells) if insider else None,
        "source": "FMP" if fmp_key else "none",
    }


# ── Entry point pubblico ──────────────────────────────────────────────────────
def fetch_fundamental_score(
    symbol:    str,
    asset_type:str,  # "stock" | "etf" | "index"
    fmp_key:   str = "",
    force_refresh: bool = False,
    current_price: float = None,  # prezzo corrente per Fair Value upside
) -> Dict:
    """
    Recupera e calcola fundamental_score e institutional_score.
    Cache 12 ore per simbolo. Graceful degradation se FMP non configurato.
    """
    cache_key = f"{symbol}_{asset_type}"
    if not force_refresh and cache_key in _CACHE:
        if (time.time() - _CACHE[cache_key]["ts"]) < CACHE_TTL:
            log.debug(f"[FUND] {symbol}: cache hit")
            return _CACHE[cache_key]["data"]

    if not fmp_key:
        result = {
            "symbol":            symbol,
            "fundamental_score": 0,
            "institutional_score": 0,
            "fundamental_detail": {"reasons": ["FMP key non configurata — fondamentali non disponibili"]},
            "institutional_detail": {},
            "source": "none",
        }
        _CACHE[cache_key] = {"data": result, "ts": time.time()}
        return result

    log.info(f"[FUND] {symbol} ({asset_type}): fetching FMP...")

    if asset_type == "stock":
        f_score, f_detail = _score_stock(symbol, fmp_key)
    elif asset_type == "etf":
        f_score, f_detail = _score_etf(symbol, fmp_key)
    else:
        f_score, f_detail = 0, {"reasons": ["Indice — fondamentali non applicabili"]}

    i_score, i_detail = _institutional_score(symbol, fmp_key)

    log.info(f"[FUND] {symbol}: fund_score={f_score:+d} inst_score={i_score:+d}")

    # Fair Value e Health Score (solo azioni con prezzo disponibile)
    fv_data     = {}
    health_data = {}
    if asset_type == "stock" and f_detail.get("_metrics_raw"):
        mx = f_detail.get("_metrics_raw", {})
        price_for_fv = current_price or mx.get("priceAvg200") or 1.0
        fv_data     = _estimate_fair_value(mx, price_for_fv)
        health_data = _financial_health_score(mx, [])

    result = {
        "symbol":               symbol,
        "fundamental_score":    f_score,
        "institutional_score":  i_score,
        "fundamental_detail":   f_detail,
        "institutional_detail": i_detail,
        "fair_value":           fv_data,
        "health_score":         health_data,
        "source": "FMP",
        "timestamp": __import__('datetime').datetime.now().isoformat(),
    }
    _CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


def fetch_all_fundamentals(
    assets: list, fmp_key: str,
    prices_map: Dict[str, float] = None  # {symbol: prezzo_corrente}
) -> Dict[str, Dict]:
    """Fetch fondamentali per tutti gli asset in sequenza."""
    results = {}
    for asset in assets:
        sym  = asset["symbol"]
        atyp = asset.get("asset_type", "stock")
        if atyp == "index":
            results[sym] = {"symbol": sym, "fundamental_score": 0, "institutional_score": 0,
                            "fundamental_detail": {"reasons": ["Indice"]}, "institutional_detail": {}}
            continue
        price = (prices_map or {}).get(sym)
        results[sym] = fetch_fundamental_score(
            sym, atyp, fmp_key, current_price=price
        )
        __import__('time').sleep(0.3)  # rate limit FMP
    return results
