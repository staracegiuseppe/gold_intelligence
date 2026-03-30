# signal_engine.py - Gold Intelligence v1.5.0
# Pipeline completa con logging dettagliato HA-compatibile
#
# FLUSSO:
#   1. [MARKET]     Fetch dati reali Yahoo Finance
#   2. [QUANT]      Calcolo indicatori tecnici + Smart Money Score
#   3. [CLAUDE]     Analisi batch - bias + livelli tecnici
#   4. [PERPLEXITY] Validazione real-time news + conferma/contraddizione
#   5. [SIGNAL]     Segnale finale BUY/SELL/HOLD con entry, SL, TP
#   6. [WINDOW]     Attivo solo 07:30-23:00 (orario trading europeo)

import os
import json
import logging
import requests
from datetime import datetime, time as dtime
from typing   import Dict, List, Optional, Any

log = logging.getLogger("signal_engine")

# ── Orario trading ────────────────────────────────────────────────────────────
TRADING_START = dtime(7, 30)
TRADING_END   = dtime(23, 0)

def is_trading_hours() -> bool:
    now = datetime.now().time()
    return TRADING_START <= now <= TRADING_END


# ── Logger strutturato per HA ─────────────────────────────────────────────────
class HALogger:
    """
    Ogni riga di log ha un prefisso visibile nel Registro HA:
    [STEP 1/6 MARKET]  GLD: fetch OK prezzo=221.50 RSI=67.2
    [STEP 2/6 QUANT]   GLD: score=+64 breakdown: ETF+20 COT+25 Yields+10 USD+15 Trend-6
    [STEP 3/6 CLAUDE]  GLD: bias=bullish conf=78% MA_cross=golden RSI=67 vs_MA200=+9.8%
    [STEP 4/6 PPLX]    GLD: CONFERMATO - gold rally continua, ETF inflows record settimana
    [STEP 5/6 SIGNAL]  GLD: *** BUY *** entry=221.50 SL=211.53 TP=232.13 RR=1:1.07
    [STEP 6/6 WINDOW]  Trading attivo 07:30-23:00 | ora=14:32 | segnali attivi: 3
    """

    def step(self, n: int, total: int, tag: str, msg: str):
        prefix = f"[STEP {n}/{total} {tag.upper()}]"
        log.info(f"{prefix}  {msg}")

    def signal(self, sym: str, action: str, entry: float, sl: float, tp: float, conf: int, reason: str):
        if action in ("BUY", "SELL"):
            rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
            log.info(
                f"[SIGNAL ★★★]  {sym}: *** {action} ***"
                f"  entry={entry:.2f}  SL={sl:.2f}  TP={tp:.2f}"
                f"  R:R=1:{rr:.2f}  conf={conf}%"
                f"  | {reason[:80]}"
            )
        else:
            log.info(f"[SIGNAL ---]  {sym}: HOLD  conf={conf}%  | {reason[:80]}")

    def warn(self, tag: str, msg: str):
        log.warning(f"[{tag.upper()}]  {msg}")

    def error(self, tag: str, msg: str):
        log.error(f"[{tag.upper()}]  {msg}")


ha = HALogger()


# ── Step 1: MARKET - fetch dati + indicatori ─────────────────────────────────
def step_market(asset: Dict, tech_data: Dict) -> Optional[Dict]:
    sym  = asset["symbol"]
    tech = tech_data.get(sym)

    if tech:
        ha.step(1, 6, "MARKET", (
            f"{sym}: fetch OK"
            f"  prezzo={tech['last_price']}"
            f"  Δ1d={tech['change_pct']:+.1f}%"
            f"  RSI={tech['rsi']}"
            f"  BB_pos={tech['bollinger']['position']:.0f}%"
            f"  volume={tech['volume']['signal']}"
            f"  MA_cross={tech['ma']['cross_signal'][:30]}"
        ))
    else:
        ha.step(1, 6, "MARKET", f"{sym}: dati reali non disponibili - uso simulazione")

    return tech


# ── Step 2: QUANT - Smart Money Score con breakdown ───────────────────────────
def step_quant(asset: Dict, d: Dict, sd: Dict) -> int:
    sym   = asset["symbol"]
    score = sd["score"]
    bd    = sd["breakdown"]

    parts = [b.get("f", b.get("l","?"))[:8] + ("+" if b.get("d", b.get("v",0)) >= 0 else "") + str(b.get("d", b.get("v",0))) for b in bd]
    ha.step(2, 6, "QUANT", (
        f"{sym}: score={score:+d}/100"
        f"  [{' | '.join(parts)}]"
        f"  triggered={'YES' if abs(score) > 30 else 'NO (sotto soglia)'}"
    ))
    return score


# ── Step 3: CLAUDE - analisi tecnica ─────────────────────────────────────────
def step_claude(rows: List[Dict], api_key: str) -> Dict:
    """Una chiamata Claude per tutti gli asset - ritorna {symbol: analysis}"""
    if not api_key:
        ha.warn("CLAUDE", "API key assente - analisi saltata")
        return {}

    today = datetime.now().strftime("%d %B %Y")
    sections = []

    for row in rows:
        sym   = row["symbol"]
        score = row["score"]
        tech  = row.get("tech")
        lines = []

        if tech:
            bb   = tech["bollinger"]
            ma   = tech["ma"]
            macd = tech["macd"]
            sto  = tech["stochastic"]
            vol  = tech["volume"]
            sr   = tech["support_res"]
            p    = tech["performance"]

            rsi_tag = (" IPERCOMPRATO" if tech["rsi"] > 70
                       else " IPERVENDUTO" if tech["rsi"] < 30 else "")
            lines += [
                "RSI=" + str(tech["rsi"]) + rsi_tag,
                "BB pos=" + str(bb["position"]) + "% " + bb["signal"]
                    + " upper=" + str(bb["upper"]) + " lower=" + str(bb["lower"])
                    + " bw=" + str(bb["bandwidth"]) + "%",
                ma["cross_signal"]
                    + " | MA20=" + str(ma["ma20"])
                    + " MA50=" + str(ma["ma50"])
                    + " MA200=" + str(ma.get("ma200", "n/a"))
                    + " | vs_MA200=" + str(ma.get("price_vs_ma200", "n/a")) + "%",
                "MACD trend=" + macd["trend"] + " hist=" + str(macd["histogram"])
                    + (" [" + macd["crossing"] + "]" if macd["crossing"] != "nessuno" else ""),
                "Stocastico K=" + str(sto["k"]) + " D=" + str(sto["d"]) + " " + sto["signal"],
                "Volume " + vol["signal"] + " " + str(vol["ratio_pct"]) + "% vs 20gg",
                "Supporto=" + str(sr["support"]) + " Resistenza=" + str(sr["resistance"]),
                "Perf 1d=" + str(p["1d"]) + "% 5d=" + str(p["5d"])
                    + "% 20d=" + str(p["20d"]) + "%",
            ]
            if row.get("signals"):
                lines.append("Flussi: " + " | ".join(row["signals"][:2]))

            hdr = ("### " + sym + " | Prezzo=" + str(tech["last_price"])
                   + " | SmartMoney=" + ("+" if score >= 0 else "") + str(score)
                   + "/100 [REALE]")
        else:
            lines = [
                "Dati tecnici non disponibili",
                "Flussi: " + " | ".join(row.get("signals", [])[:2]),
            ]
            hdr = ("### " + sym
                   + " | SmartMoney=" + ("+" if score >= 0 else "") + str(score)
                   + "/100 [simulato]")

        sections.append(hdr + "\n" + "\n".join("  - " + l for l in lines))

    prompt = (
        "You are a senior quantitative analyst. Today is " + today + ".\n\n"
        "Analyze these assets using REAL Yahoo Finance indicators + Smart Money Scores.\n\n"
        "ASSET DATA:\n\n" + "\n\n".join(sections) + "\n\n"
        "RULES:\n"
        "- SmartMoney >+30=bull | <-30=bear\n"
        "- RSI>70 near BB upper = overextended, reversal risk\n"
        "- RSI<30 near BB lower = oversold bounce\n"
        "- MACD histogram crossing zero = momentum shift\n"
        "- High volume on rally = confirmation\n"
        "- For each asset, identify entry zone, stop loss, take profit from S/R levels\n\n"
        '{"results": [{'
        '"symbol":"GLD",'
        '"bias":"bullish|bearish|neutral",'
        '"confidence":0-100,'
        '"summary":"2-3 sentences with specific values",'
        '"entry_zone":"price level",'
        '"stop_loss":"price level",'
        '"take_profit":"price level",'
        '"primary_driver":"main signal",'
        '"tech_signal":"strongest technical",'
        '"smart_money_alignment":"aligned|divergent|mixed",'
        '"risk":"main risk",'
        '"drivers":["factor=value"],'
        '"time_horizon":"short-term|medium-term|long-term"'
        "}]}"
    )

    ha.step(3, 6, "CLAUDE", f"invio batch {len(rows)} asset | ~{len(prompt)//4} token")

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model":      "claude-sonnet-4-20250514",
                "max_tokens": 3000,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        if r.status_code == 200:
            text      = r.json()["content"][0]["text"]
            clean     = text.replace("```json", "").replace("```", "").strip()
            data      = json.loads(clean)
            ai_map    = {x["symbol"]: x for x in data.get("results", [])}

            for sym, a in ai_map.items():
                ha.step(3, 6, "CLAUDE", (
                    f"{sym}: bias={a.get('bias','?')}"
                    f"  conf={a.get('confidence','?')}%"
                    f"  entry={a.get('entry_zone','?')}"
                    f"  SL={a.get('stop_loss','?')}"
                    f"  TP={a.get('take_profit','?')}"
                    f"  tech={a.get('tech_signal','?')[:40]}"
                    f"  alignment={a.get('smart_money_alignment','?')}"
                ))
            return ai_map

        ha.error("CLAUDE", f"HTTP {r.status_code}: {r.text[:150]}")
        return {}

    except json.JSONDecodeError as e:
        ha.error("CLAUDE", f"JSON parse error: {e}")
        return {}
    except Exception as e:
        ha.error("CLAUDE", f"Errore: {e}")
        return {}


# ── Step 4: PERPLEXITY - validazione real-time ────────────────────────────────
def step_perplexity(symbol: str, name: str, claude_bias: str,
                    claude_summary: str, tech: Optional[Dict],
                    pplx_key: str) -> Dict:
    """
    Perplexity cerca notizie e dati real-time per validare o contraddire
    il bias di Claude. Usa sonar-pro con search_recency_filter=day/week.
    """
    if not pplx_key:
        ha.step(4, 6, "PPLX", f"{symbol}: key assente - validazione saltata")
        return {"validation": "skipped", "content": "", "citations": [], "verdict": "unknown"}

    price_str = ""
    if tech:
        price_str = f" (prezzo={tech['last_price']}, RSI={tech['rsi']})"

    query = (
        f"Real-time market analysis for {name} ({symbol}){price_str}. "
        f"My model says: {claude_bias} - {claude_summary[:120]}. "
        f"Find TODAY's news, price action, institutional flows, analyst calls, "
        f"or macro events that CONFIRM or CONTRADICT this {claude_bias} thesis. "
        f"Be specific with data points and sources."
    )

    ha.step(4, 6, "PPLX", f"{symbol}: ricerca real-time (bias Claude={claude_bias})")

    try:
        r = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {pplx_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model":                 "sonar-pro",
                "messages": [
                    {
                        "role":    "system",
                        "content": (
                            "You are a real-time market analyst. "
                            "Search for the latest news and data. "
                            "State clearly: CONFIRMED, CONTRADICTED, or MIXED. "
                            "Be concise and cite specific facts."
                        ),
                    },
                    {"role": "user", "content": query},
                ],
                "max_tokens":             600,
                "temperature":            0.1,
                "search_recency_filter":  "day",
                "return_citations":       True,
            },
            timeout=25,
        )

        if r.status_code == 200:
            j         = r.json()
            content   = j["choices"][0]["message"]["content"]
            citations = j.get("citations", [])

            # Determina verdetto
            upper = content.upper()
            if "CONFIRM" in upper or "CONFERMA" in upper or "SUPPORTS" in upper:
                verdict = "CONFIRMED"
            elif "CONTRADICT" in upper or "CONTRADDIC" in upper or "OPPOSES" in upper:
                verdict = "CONTRADICTED"
            else:
                verdict = "MIXED"

            # Log sintetico
            preview = content[:120].replace("\n", " ")
            ha.step(4, 6, "PPLX", (
                f"{symbol}: {verdict}"
                f"  cite={len(citations)}"
                f"  | {preview}..."
            ))

            return {
                "validation": verdict,
                "content":    content,
                "citations":  citations,
                "verdict":    verdict,
            }

        ha.warn("PPLX", f"{symbol}: HTTP {r.status_code} - {r.text[:100]}")
        return {"validation": "error", "content": "", "citations": [], "verdict": "unknown"}

    except Exception as e:
        ha.warn("PPLX", f"{symbol}: errore {e}")
        return {"validation": "error", "content": "", "citations": [], "verdict": "unknown"}


# ── Step 5: SIGNAL - segnale finale ──────────────────────────────────────────
def step_signal(
    symbol:     str,
    score:      int,
    claude_a:   Dict,
    pplx_r:     Dict,
    tech:       Optional[Dict],
) -> Dict:
    """
    Combina score quant + Claude + Perplexity in un segnale BUY/SELL/HOLD
    con entry, stop loss, take profit e Risk:Reward.
    """
    bias      = claude_a.get("bias", "neutral")
    conf_base = int(claude_a.get("confidence", 50))
    verdict   = pplx_r.get("verdict", "unknown")
    alignment = claude_a.get("smart_money_alignment", "mixed")

    # ── Confidence adjustment ──────────────────────────────────────────────────
    conf = conf_base
    if verdict == "CONFIRMED":
        conf = min(conf + 12, 99)
    elif verdict == "CONTRADICTED":
        conf = max(conf - 20, 10)

    if alignment == "aligned":
        conf = min(conf + 8, 99)
    elif alignment == "divergent":
        conf = max(conf - 10, 10)

    if abs(score) > 60:
        conf = min(conf + 5, 99)

    # ── Azione ────────────────────────────────────────────────────────────────
    if bias == "bullish" and conf >= 60 and verdict != "CONTRADICTED" and score > 20:
        action = "BUY"
    elif bias == "bearish" and conf >= 60 and verdict != "CONTRADICTED" and score < -20:
        action = "SELL"
    else:
        action = "HOLD"

    # ── Livelli prezzo ────────────────────────────────────────────────────────
    # Priorità: livelli da Claude (calcolati su S/R reali) > tecnici > stima %
    entry = sl = tp = None

    if tech:
        price = tech["last_price"]
        sr    = tech["support_res"]

        # Entry = prezzo corrente (o leggermente sotto/sopra per limit order)
        if action == "BUY":
            entry = price
            sl    = sr["support"]
            tp    = sr["resistance"]
        elif action == "SELL":
            entry = price
            sl    = sr["resistance"]
            tp    = sr["support"]

        # Override con livelli Claude se disponibili e numerici
        for field, attr in [
            ("entry_zone",  "entry"),
            ("stop_loss",   "sl"),
            ("take_profit", "tp"),
        ]:
            raw = claude_a.get(field, "")
            try:
                parsed = float(str(raw).replace("$", "").replace(",", "").split()[0])
                if 0 < parsed < price * 3:
                    if attr == "entry": entry = parsed
                    elif attr == "sl":  sl    = parsed
                    elif attr == "tp":  tp    = parsed
            except (ValueError, IndexError, AttributeError):
                pass

    # ── R:R ratio ─────────────────────────────────────────────────────────────
    rr = None
    if entry and sl and tp and abs(entry - sl) > 0:
        rr = round(abs(tp - entry) / abs(entry - sl), 2)

    # Motivo sintetico per il log
    reason_parts = [
        "score=" + ("+" if score >= 0 else "") + str(score),
        "Claude=" + bias + "(" + str(conf_base) + "%)",
        "PPLX=" + verdict,
        "align=" + alignment,
    ]
    reason = " | ".join(reason_parts)

    # Log segnale
    ha.signal(
        symbol, action,
        entry or 0, sl or 0, tp or 0,
        conf, reason
    )

    return {
        "symbol":     symbol,
        "action":     action,
        "confidence": conf,
        "entry":      round(entry, 2) if entry else None,
        "stop_loss":  round(sl, 2)    if sl    else None,
        "take_profit":round(tp, 2)    if tp    else None,
        "risk_reward":rr,
        "bias":       bias,
        "score":      score,
        "pplx_verdict":   verdict,
        "pplx_content":   pplx_r.get("content", ""),
        "pplx_citations": pplx_r.get("citations", []),
        "alignment":      alignment,
        "analysis":       claude_a,
        "timestamp":      datetime.now().isoformat(),
        "reason":         reason,
    }


# ── Step 6: WINDOW - controllo orario ────────────────────────────────────────
def step_window(signals: List[Dict]) -> List[Dict]:
    now    = datetime.now()
    active = is_trading_hours()
    buys   = [s for s in signals if s["action"] == "BUY"]
    sells  = [s for s in signals if s["action"] == "SELL"]
    holds  = [s for s in signals if s["action"] == "HOLD"]

    ha.step(6, 6, "WINDOW", (
        f"ora={now.strftime('%H:%M')}"
        f"  finestra={'APERTA 07:30-23:00' if active else 'CHIUSA - segnali congelati'}"
        f"  BUY={len(buys)}  SELL={len(sells)}  HOLD={len(holds)}"
        f"  top_buy={buys[0]['symbol'] + ' conf=' + str(buys[0]['confidence']) + '%' if buys else 'nessuno'}"
        f"  top_sell={sells[0]['symbol'] + ' conf=' + str(sells[0]['confidence']) + '%' if sells else 'nessuno'}"
    ))

    # Fuori orario: converte tutto in HOLD ma mantiene i livelli
    if not active:
        for s in signals:
            s["action_effective"] = "HOLD (fuori orario)"
            s["trading_window"]   = False
    else:
        for s in signals:
            s["action_effective"] = s["action"]
            s["trading_window"]   = True

    return signals


# ── PIPELINE COMPLETA ─────────────────────────────────────────────────────────
def run_full_pipeline(
    assets:      List[Dict],
    tech_data:   Dict,
    score_map:   Dict,   # {symbol: {score, breakdown, signals, data}}
    claude_key:  str,
    pplx_key:    str,
    threshold:   int = 30,
) -> List[Dict]:
    """
    Esegue la pipeline completa per tutti gli asset.
    Ritorna lista di segnali ordinati per confidence.
    """
    now = datetime.now()
    log.info("=" * 60)
    log.info(f"[PIPELINE START]  {now.strftime('%Y-%m-%d %H:%M:%S')}"
             f"  asset={len(assets)}"
             f"  finestra={'APERTA' if is_trading_hours() else 'CHIUSA'}")
    log.info("=" * 60)

    # Step 1: Market data (già fetchato, solo logging)
    for asset in assets:
        step_market(asset, tech_data)

    # Step 2: Quant score per tutti
    for sm in score_map.values():
        if sm.get("asset"):
            step_quant(sm["asset"], sm["data"], sm["score_data"])

    # Step 3: Claude batch (solo asset triggered)
    triggered_rows = [
        sm["row"] for sm in score_map.values()
        if sm.get("row") and abs(sm["row"]["score"]) > threshold
    ]

    claude_map = {}
    if triggered_rows:
        claude_map = step_claude(triggered_rows, claude_key)
    else:
        log.info("[STEP 3/6 CLAUDE]  Nessun asset sopra soglia - analisi saltata")

    # Step 4+5: Perplexity + Signal per ogni asset
    signals = []
    for asset in assets:
        sym      = asset["symbol"]
        sm       = score_map.get(sym, {})
        score    = sm.get("score", 0)
        tech     = tech_data.get(sym)
        claude_a = claude_map.get(sym, {})
        row      = sm.get("row", {})

        if not claude_a:
            # Nessuna analisi Claude - signal HOLD
            ha.step(4, 6, "PPLX",   f"{sym}: skip (nessuna analisi Claude)")
            ha.step(5, 6, "SIGNAL", f"{sym}: HOLD (score sotto soglia o no Claude)")
            signals.append({
                "symbol": sym, "action": "HOLD", "confidence": 40,
                "entry": None, "stop_loss": None, "take_profit": None,
                "risk_reward": None, "bias": "neutral", "score": score,
                "pplx_verdict": "skipped", "pplx_content": "",
                "pplx_citations": [], "alignment": "mixed",
                "analysis": {}, "timestamp": datetime.now().isoformat(),
                "reason": "score=" + str(score) + " sotto soglia o nessuna analisi AI",
                "action_effective": "HOLD", "trading_window": is_trading_hours(),
                "has_real_data": tech is not None,
                "name": asset.get("name",""), "type": asset.get("type",""),
                "sector": asset.get("sector",""),
            })
            continue

        # Perplexity validation
        pplx_r = step_perplexity(
            sym,
            asset.get("name", sym),
            claude_a.get("bias", "neutral"),
            claude_a.get("summary", ""),
            tech,
            pplx_key,
        )

        # Signal finale
        sig = step_signal(sym, score, claude_a, pplx_r, tech)
        sig["has_real_data"] = tech is not None
        sig["name"]   = asset.get("name", "")
        sig["type"]   = asset.get("type", "")
        sig["sector"] = asset.get("sector", "")
        if row.get("signals"):
            sig["quant_signals"] = row["signals"]
        signals.append(sig)

    # Step 6: Window check
    signals = step_window(signals)

    # Ordina: BUY/SELL per confidence desc, poi HOLD
    active   = [s for s in signals if s["action"] in ("BUY", "SELL")]
    inactive = [s for s in signals if s["action"] == "HOLD"]
    active.sort(key=lambda s: s["confidence"], reverse=True)
    signals  = active + inactive

    log.info("=" * 60)
    log.info(f"[PIPELINE DONE]  segnali attivi: {len(active)}"
             f"  TOP={active[0]['symbol'] + ' ' + active[0]['action'] if active else 'nessuno'}")
    log.info("=" * 60)

    return signals
