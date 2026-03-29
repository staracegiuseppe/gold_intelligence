# gold_engine.py — Gold Intelligence System — Dual Engine Configurabile
#
# MODALITÀ:
#   "dual"   → Perplexity sonar-pro (grounding) + Claude (synthesis)
#   "claude" → Claude + web_search_20250305 (fallback automatico)
#
# FALLBACK: se PERPLEXITY_API_KEY non è settata, passa automaticamente a "claude"
#
# USO:
#   result = run_pipeline(data)          # auto-detect modalità da env
#   result = run_pipeline(data, mode="dual")
#   result = run_pipeline(data, mode="claude")

import os, json, time, hashlib, logging
from typing import Dict, Optional
from datetime import datetime, timedelta
import requests

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")   # opzionale

CLAUDE_URL      = "https://api.anthropic.com/v1/messages"
PERPLEXITY_URL  = "https://api.perplexity.ai/chat/completions"
CLAUDE_MODEL    = "claude-sonnet-4-20250514"
PPLX_MODEL      = "sonar-pro"

SCORE_THRESHOLD = 30
CACHE_TTL_MIN   = 60
MAX_RETRIES     = 3
TIMEOUT_PPLX    = 20
TIMEOUT_CLAUDE  = 30   # web search needs longer timeout

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] gold_engine: %(message)s")
log = logging.getLogger("gold_engine")

# ─── CACHE ────────────────────────────────────────────────────────────────────
_cache: Dict[str, Dict] = {}

def _ckey(obj):
    return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:14]

def _cget(k):
    e = _cache.get(k)
    if e and datetime.utcnow() < e["exp"]:
        log.info(f"Cache HIT [{k}]")
        return e["v"]

def _cset(k, v):
    _cache[k] = {"v": v, "exp": datetime.utcnow() + timedelta(minutes=CACHE_TTL_MIN)}

# ─── QUANT ENGINE ─────────────────────────────────────────────────────────────
def compute_score(d: Dict) -> Dict:
    s = 0; b = []
    etf = float(d.get("etf_flows", 0))
    w   = round(min(30, abs(etf) / 30))
    (s := s + w, b.append({"f":"etf","d":+w,"n":f"ETF inflows +{etf:.0f}M"})) if etf > 0 \
    else (s := s - w, b.append({"f":"etf","d":-w,"n":f"ETF outflows {etf:.0f}M"}))
    cot = float(d.get("cot_positioning", 0))
    (s := s + 25, b.append({"f":"cot","d":+25,"n":f"COT net long +{cot/1e3:.0f}K"})) if cot > 0 \
    else (s := s - 25, b.append({"f":"cot","d":-25,"n":f"COT net short {cot/1e3:.0f}K"}))
    ry = float(d.get("real_yields", 0))
    if   ry < -.5: s += 20; b.append({"f":"ry","d":+20,"n":"Yields deeply negative"})
    elif ry < 0:   s += 10; b.append({"f":"ry","d":+10,"n":"Yields mildly negative"})
    elif ry < .5:  s -= 10; b.append({"f":"ry","d":-10,"n":"Yields mildly positive"})
    else:          s -= 20; b.append({"f":"ry","d":-20,"n":"Yields high — headwind"})
    usd = d.get("usd_trend", "flat")
    if usd == "down": s += 15; b.append({"f":"usd","d":+15,"n":"USD weakening"})
    elif usd == "up": s -= 15; b.append({"f":"usd","d":-15,"n":"USD strengthening"})
    gt = d.get("gold_trend", "sideways")
    if gt == "up":   s += 10; b.append({"f":"gt","d":+10,"n":"Gold uptrend"})
    elif gt == "down":s -= 10; b.append({"f":"gt","d":-10,"n":"Gold downtrend"})
    return {"score": max(-100, min(100, round(s))), "breakdown": b}


def enrich_signals(d: Dict) -> list[str]:
    etf = float(d.get("etf_flows", 0))
    cot = float(d.get("cot_positioning", 0))
    ry  = float(d.get("real_yields", 0))
    return [s for s in [
        (f"Aggressive ETF accumulation +{etf:.0f}M" if etf > 500
         else f"Moderate ETF inflows +{etf:.0f}M" if etf > 0
         else f"ETF outflows {etf:.0f}M" if etf > -500
         else f"Heavy ETF distribution {etf:.0f}M"),
        (f"Hedge funds extremely net long +{cot/1e3:.0f}K — crowded bull" if cot > 150000
         else f"COT net long +{cot/1e3:.0f}K" if cot > 0
         else f"COT net short {cot/1e3:.0f}K"),
        (f"Real yields deeply negative {ry:+.2f}% — strong tailwind" if ry < -.5
         else f"Real yields mildly negative {ry:+.2f}%" if ry < 0
         else f"Real yields positive +{ry:.2f}% — headwind"),
        {"down": "USD weakening — gold bid", "up": "USD strengthening — gold cap",
         "flat": "USD flat — neutral"}.get(d.get("usd_trend", "flat"), ""),
        f"Macro: {d.get('macro_event', 'none')}",
    ] if s]


# ─── PERPLEXITY ENGINE ────────────────────────────────────────────────────────
def call_perplexity(d: Dict, score: int, signals: list[str]) -> Dict:
    ck = _ckey({"pplx": d, "score": score})
    if c := _cget(ck): return {**c, "_cached": True}

    direction = "bullish" if score > 0 else "bearish"
    query = (
        f"Gold XAU/USD macro analysis — {direction} signal at {score:+d}/100.\n"
        f"Quant signals: {'; '.join(signals[:4])}\n"
        f"Market: ETF {d.get('etf_flows')}M, COT {float(d.get('cot_positioning',0))/1e3:.0f}K, "
        f"yields {d.get('real_yields')}%, USD {d.get('usd_trend')}, macro: {d.get('macro_event')}\n\n"
        f"Find current data (Fed, USD index, GLD/IAU flows, TIPS, gold price) that "
        f"CONFIRMS or CONTRADICTS the {direction} signals. Be factual, cite sources."
    )

    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": PPLX_MODEL, "max_tokens": 800, "temperature": 0.1,
        "search_recency_filter": "week", "return_citations": True,
        "messages": [
            {"role": "system", "content": "You are a gold market macro analyst. Be factual, technical, cite data."},
            {"role": "user", "content": query},
        ],
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Perplexity call attempt {attempt}")
            r = requests.post(PERPLEXITY_URL, headers=headers, json=payload, timeout=TIMEOUT_PPLX)
            if r.status_code == 200:
                j = r.json()
                result = {
                    "content":   j["choices"][0]["message"]["content"],
                    "citations": j.get("citations", []),
                    "tokens":    j.get("usage", {}).get("total_tokens", 0),
                }
                log.info(f"Perplexity OK — {len(result['citations'])} citations")
                _cset(ck, result)
                return result
            elif r.status_code == 429:
                time.sleep(2 ** attempt)
            else:
                log.error(f"Perplexity {r.status_code}: {r.text[:200]}")
                time.sleep(2)
        except requests.Timeout:
            log.warning(f"Perplexity timeout attempt {attempt}"); time.sleep(2)
        except Exception as e:
            log.error(f"Perplexity error: {e}"); time.sleep(2)

    raise RuntimeError("Perplexity: all retries failed")


# ─── CLAUDE ENGINE ────────────────────────────────────────────────────────────
CLAUDE_HEADERS = lambda: {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "content-type": "application/json",
}

WEB_SEARCH_TOOL = {"type": "web_search_20250305", "name": "web_search"}

SYNTHESIS_PROMPT = lambda d, score, signals, web_ctx: (
    f"You are a quantitative macro analyst for gold markets.\n\n"
    f"SMART MONEY SCORE: {score:+d}/100\n"
    f"MARKET DATA:\n"
    f"- Gold trend: {d.get('gold_trend')}\n"
    f"- ETF flows 7d: {d.get('etf_flows')}M USD\n"
    f"- COT: {float(d.get('cot_positioning',0))/1e3:.0f}K contracts\n"
    f"- Real yields Δ7d: {d.get('real_yields')}%\n"
    f"- USD: {d.get('usd_trend')}\n"
    f"- Macro: {d.get('macro_event')}\n\n"
    f"PRE-COMPUTED SIGNALS:\n{chr(10).join(f'{i+1}. {s}' for i,s in enumerate(signals))}\n\n"
    f"REAL-TIME MACRO CONTEXT (validated web data):\n{web_ctx}\n\n"
    f"Check if web data confirms or contradicts quant signals.\n"
    f"Respond with ONLY valid JSON (no markdown):\n"
    '{"bias":"bullish|bearish|neutral","confidence":0-100,"validation":"confirmed|contradicted|mixed",'
    '"summary":"<3 sentences>","convergence_score":0-100,"web_findings":["f1","f2"],'
    '"drivers":["d1","d2"],"risk_factors":["r1","r2"],"contradictions":["c1"],'
    '"time_horizon":"short-term|medium-term|long-term"}'
)


def _claude_post(payload: Dict, timeout: int = TIMEOUT_CLAUDE) -> Dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(CLAUDE_URL, headers=CLAUDE_HEADERS(), json=payload, timeout=timeout)
            if r.status_code == 200: return r.json()
            if r.status_code == 429: time.sleep(2 ** attempt)
            else: log.error(f"Claude {r.status_code}: {r.text[:200]}"); time.sleep(2)
        except requests.Timeout:
            log.warning(f"Claude timeout attempt {attempt}"); time.sleep(2)
        except Exception as e:
            log.error(f"Claude error: {e}"); time.sleep(2)
    raise RuntimeError("Claude: all retries failed")


def call_claude_web_search(d: Dict, score: int, signals: list[str]) -> Dict:
    """Claude mode: use web_search tool to ground the analysis."""
    user_msg = (
        f"Score: {score:+d}/100. Market: ETF {d.get('etf_flows')}M, "
        f"COT {float(d.get('cot_positioning',0))/1e3:.0f}K, yields {d.get('real_yields')}%, "
        f"USD {d.get('usd_trend')}, macro: {d.get('macro_event')}.\n"
        f"Signals: {'; '.join(signals[:4])}.\n\n"
        f"Search for current gold market conditions (Fed, USD, ETF flows, yields, price action). "
        f'Return JSON: {{"content":"<macro summary>","web_searches_done":["q1","q2"]}}'
    )

    r1 = _claude_post({
        "model": CLAUDE_MODEL, "max_tokens": 1500,
        "system": "You are a macro analyst. Use web search, return only valid JSON.",
        "tools": [WEB_SEARCH_TOOL],
        "messages": [{"role": "user", "content": user_msg}],
    })

    tool_uses = [b for b in r1.get("content", []) if b.get("type") == "tool_use"]
    searches  = [b.get("input", {}).get("query", "") for b in tool_uses]

    if r1.get("stop_reason") == "tool_use" and tool_uses:
        tool_results = [{"type": "tool_result", "tool_use_id": b["id"],
                         "content": "Search executed. Return the JSON."} for b in tool_uses]
        r2 = _claude_post({
            "model": CLAUDE_MODEL, "max_tokens": 1500,
            "system": "You are a macro analyst. Return only valid JSON.",
            "tools": [WEB_SEARCH_TOOL],
            "messages": [
                {"role": "user", "content": user_msg},
                {"role": "assistant", "content": r1["content"]},
                {"role": "user", "content": tool_results},
            ],
        })
        text = next((b["text"] for b in r2.get("content", []) if b.get("type") == "text"), "{}")
    else:
        text = next((b["text"] for b in r1.get("content", []) if b.get("type") == "text"), "{}")

    try:
        parsed = json.loads(text.replace("```json", "").replace("```", "").strip())
    except json.JSONDecodeError:
        parsed = {"content": text[:500]}
    parsed["web_searches_done"] = searches
    return parsed


def call_claude_synthesis(d: Dict, score: int, signals: list[str], web_ctx: str) -> Dict:
    """Claude final synthesis (called in both modes)."""
    ck = _ckey({"syn": d, "score": score, "ctx": web_ctx[:80]})
    if c := _cget(ck): return {**c, "_cached": True}

    r = _claude_post({
        "model": CLAUDE_MODEL, "max_tokens": 800,
        "messages": [{"role": "user", "content": SYNTHESIS_PROMPT(d, score, signals, web_ctx)}],
    })
    text = next((b["text"] for b in r.get("content", []) if b.get("type") == "text"), "{}")
    result = json.loads(text.replace("```json", "").replace("```", "").strip())
    _cset(ck, result)
    return result


# ─── PIPELINE ─────────────────────────────────────────────────────────────────
def effective_mode(requested: str = "auto") -> str:
    """
    Determina la modalità effettiva.
    Legge PERPLEXITY_API_KEY a runtime (non all'import) per supportare
    env vars settate dinamicamente o caricate da .env dopo l'import.
    - "auto"   → dual se PERPLEXITY_API_KEY settata, altrimenti claude
    - "dual"   → forza dual (fallback se key mancante)
    - "claude" → forza claude + web_search
    """
    pplx_key = os.getenv("PERPLEXITY_API_KEY")
    if requested == "auto":
        return "dual" if pplx_key else "claude"
    if requested == "dual" and not pplx_key:
        log.warning("Dual mode richiesta ma PERPLEXITY_API_KEY assente → fallback a claude")
        return "claude"
    return requested


def run_pipeline(d: Dict, mode: str = "auto") -> Dict:
    """
    Pipeline completa con fallback automatico.

    Args:
        d:    market data dict
        mode: "auto" | "dual" | "claude"

    Returns:
        dict con score, signals, web_context, analysis, engine_used
    """
    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY non settata")

    ts     = datetime.utcnow().isoformat() + "Z"
    engine = effective_mode(mode)
    log.info(f"=== PIPELINE START === mode={mode} engine={engine}")

    # 1. Quant
    sd     = compute_score(d)
    score  = sd["score"]
    signals = enrich_signals(d)
    log.info(f"Score: {score:+d} | threshold: ±{SCORE_THRESHOLD} | triggered: {abs(score) > SCORE_THRESHOLD}")

    # Skip LLM if score in noise zone
    if abs(score) <= SCORE_THRESHOLD:
        log.info("Noise zone → LLM skipped")
        return {
            "timestamp": ts, "engine_used": engine, "input": d,
            "smart_money": sd, "signals": signals,
            "triggered": False, "web_context": None,
            "analysis": {
                "bias": "neutral", "confidence": 40, "validation": "mixed",
                "summary": f"Score {score:+d} sotto soglia ±{SCORE_THRESHOLD}. Nessun segnale direzionale forte.",
                "_skipped": True,
            },
        }

    # 2. Web grounding
    web_ctx  = ""
    web_meta = {}

    if engine == "dual":
        log.info("Perplexity sonar-pro → grounding…")
        pr       = call_perplexity(d, score, signals)
        web_ctx  = pr.get("content", "")
        web_meta = {"citations": pr.get("citations", []), "tokens": pr.get("tokens", 0)}
        log.info(f"Perplexity OK — {len(pr.get('citations',[]))} citations")
    else:
        log.info("Claude web_search → grounding…")
        wr       = call_claude_web_search(d, score, signals)
        web_ctx  = wr.get("content", "")
        web_meta = {"web_searches_done": wr.get("web_searches_done", [])}
        log.info(f"Claude web_search OK — searches: {web_meta['web_searches_done']}")

    # 3. Claude synthesis
    log.info("Claude synthesis…")
    analysis = call_claude_synthesis(d, score, signals, web_ctx)
    log.info(f"Claude OK — bias={analysis.get('bias')} conf={analysis.get('confidence')} val={analysis.get('validation')}")

    return {
        "timestamp":   ts,
        "engine_used": engine,
        "input":       d,
        "smart_money": sd,
        "signals":     signals,
        "triggered":   True,
        "web_context": {"text": web_ctx, **web_meta},
        "analysis":    analysis,
    }


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sample = {
        "etf_flows":       450,
        "cot_positioning": 145000,
        "real_yields":     -0.3,
        "usd_trend":       "down",
        "gold_trend":      "up",
        "macro_event":     "Fed hold expected, CPI cooling",
    }

    # Usa "auto" per rilevare automaticamente la modalità dalle env vars
    result = run_pipeline(sample, mode="auto")

    print(f"\n{'='*60}")
    print(f"  Engine used:   {result['engine_used'].upper()}")
    print(f"  Score:         {result['smart_money']['score']:+d}/100")
    print(f"  Triggered:     {result['triggered']}")
    if result["triggered"]:
        a = result["analysis"]
        print(f"  Bias:          {a.get('bias','?').upper()}")
        print(f"  Confidence:    {a.get('confidence','?')}%")
        print(f"  Validation:    {a.get('validation','?')}")
        print(f"  Convergence:   {a.get('convergence_score','?')}%")
    print(f"{'='*60}\n")
    print(json.dumps(result, indent=2, default=str))
