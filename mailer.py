"""
mailer.py
Email reporter for multi-market quant signals.

No AI required; if `ai_validation` is present, include a short summary.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List
from datetime import datetime

log = logging.getLogger("mailer")


def _col_for_action(action: str) -> str:
    if action == "BUY":
        return "#18B85A"
    if action == "SELL":
        return "#E03838"
    if action == "WATCHLIST":
        return "#C8A020"
    if action == "NO_DATA":
        return "#6880A0"
    return "#6880A0"


def _badge_for_action(action: str) -> str:
    if action == "BUY":
        return "BUY"
    if action == "SELL":
        return "SELL"
    if action == "WATCHLIST":
        return "WATCHLIST"
    if action == "NO_DATA":
        return "NO_DATA"
    return "HOLD"


def _fmt_num(x: Any) -> str:
    if x is None:
        return "—"
    try:
        if isinstance(x, str):
            return x
        return f"{float(x):.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def build_html(results: List[Dict[str, Any]], run_ts: str, next_ts: str, min_score: int) -> str:
    strong = [r for r in results if abs(float(r.get("score", 0))) >= float(min_score) and r.get("action") in ("BUY", "SELL")]
    n = len(results)
    run_dt = (run_ts[:19].replace("T", " ")) if run_ts else "---"
    next_dt = (next_ts[:19].replace("T", " ")) if next_ts else "---"

    card_rows = []
    for r in strong[:30]:
        action = r.get("action", "HOLD")
        col = _col_for_action(action)
        conf = r.get("confidence", 0)
        sym = r.get("symbol", "")
        name = r.get("name", "")
        entry = _fmt_num(r.get("entry"))
        sl = _fmt_num(r.get("stop_loss"))
        tp = _fmt_num(r.get("take_profit"))
        rr = _fmt_num(r.get("risk_reward"))
        ai = r.get("ai_validation") or {}
        ai_summary = ai.get("summary") or ""
        headlines = r.get("news_headlines") or []
        headlines_txt = ""
        if ai_summary and headlines:
            hs = headlines[:3]
            headlines_txt = "<div style='margin-top:8px;color:#7A90B0;font-size:12px;line-height:1.5'>"
            for h in hs:
                headlines_txt += f"<div>• {h.get('date','')} {h.get('source','')}: {h.get('title','')[:90]}</div>"
            headlines_txt += "</div>"

        card_html = (
            "<div style='background:#07090D;border:1px solid #1E2D45;border-radius:8px;padding:14px;margin-bottom:12px'>"
            + f"<div style='display:flex;justify-content:space-between;gap:12px;align-items:flex-start'>"
            + f"<div><div style='font-size:16px;font-weight:900;color:#D0DFF8'>{sym}</div>"
            + f"<div style='font-size:12px;color:#6880A0;margin-top:2px'>{name}</div>"
            + f"<div style='margin-top:8px'><span style='padding:3px 10px;border-radius:4px;border:1px solid {col}55;background:{col}18;color:{col};font-size:11px;font-weight:900'>{_badge_for_action(action)}</span></div>"
            + "</div>"
            + f"<div style='text-align:right'><div style='font-size:26px;font-weight:900;color:{col};font-family:monospace'>{'+' if r.get('score',0)>0 else ''}{_fmt_num(r.get('score'))}</div>"
            + f"<div style='font-size:11px;color:#6880A0'>/100 · conf {conf}%</div></div>"
            + "</div>"
            + "<div style='margin-top:10px;font-size:12px;color:#D0DFF8;line-height:1.7'>"
            + f"<div>Entry: <span style='font-family:monospace'>{entry}</span> · SL: <span style='font-family:monospace'>{sl}</span> · TP: <span style='font-family:monospace'>{tp}</span></div>"
            + f"<div>Risk/Reward: <span style='font-family:monospace'>{rr}</span></div>"
            + "</div>"
            + (f"<div style='margin-top:10px;color:#B0C4DE;font-size:12px;line-height:1.6;border-left:3px solid {col};padding-left:10px'>{ai_summary}</div>" if ai_summary else "")
            + headlines_txt
            + "</div>"
        )
        card_rows.append(card_html)

    if not card_rows:
        card_rows.append(
            "<div style='padding:14px;color:#6880A0;font-size:13px'>Nessun BUY/SELL sopra soglia (|score| >= %d).</div>" % min_score
        )

    subject_style = (
        "<div style='max-width:820px;margin:0 auto;padding:18px'>"
        "<div style='background:linear-gradient(135deg,#0B0E16,#111827);border:1px solid #1E2D45;border-radius:10px;padding:18px;margin-bottom:16px'>"
        "<div style='font-size:24px;font-weight:900;color:#C8A020'>Multi-Market Intelligence</div>"
        "<div style='font-size:12px;color:#6880A0;margin-top:6px'>Quant signal email · Quantitative levels only</div>"
        f"<div style='margin-top:10px;font-size:12px;color:#6880A0'>Generato: <b style='color:#D0DFF8'>{run_dt}</b> &nbsp; Prossimo: <b style='color:#D0DFF8'>{next_dt}</b> &nbsp; Asset totali: <b style='color:#C8A020'>{n}</b></div>"
        "</div>"
        "<div>"
    )

    end_html = (
        "</div>"
        "</div>"
    )

    return "<!DOCTYPE html><html lang='it'><head><meta charset='UTF-8'/></head><body style='margin:0;padding:0;background:#07090D;color:#D0DFF8;font-family:Segoe UI,system-ui,sans-serif'>" + subject_style + "".join(card_rows) + end_html + "</body></html>"


def send_report(results: List[Dict[str, Any]], run_ts: str, next_ts: str, cfg: Dict[str, Any]) -> bool:
    if not cfg.get("email_enabled"):
        return False

    required = ["email_to", "email_from", "smtp_host", "smtp_user", "smtp_password"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        log.warning("Email skip - mancanti: %s", missing)
        return False

    min_score = int(cfg.get("email_min_score", 40))
    html_body = build_html(results, run_ts, next_ts, min_score)

    n_strong = len([r for r in results if r.get("action") in ("BUY", "SELL") and abs(float(r.get("score", 0))) >= min_score])
    subject = "Multi-Market Intelligence - strong signals (%d) - %s" % (n_strong, (run_ts[:10] if run_ts else ""))

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["email_from"]
    msg["To"] = cfg["email_to"]
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    host = cfg.get("smtp_host", "smtp.gmail.com")
    port = int(cfg.get("smtp_port", 587))
    tls = bool(cfg.get("smtp_tls", True))
    user = cfg["smtp_user"]
    pw = cfg["smtp_password"]

    try:
        log.info("Invio email a %s via %s:%s", cfg["email_to"], host, port)
        if tls:
            srv = smtplib.SMTP(host, port, timeout=15)
            srv.ehlo()
            srv.starttls()
        else:
            srv = smtplib.SMTP_SSL(host, port, timeout=15)
        srv.login(user, pw)
        srv.sendmail(cfg["email_from"], cfg["email_to"], msg.as_string())
        srv.quit()
        log.info("Email inviata OK")
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("Auth SMTP fallita - verifica user/password")
        return False
    except Exception as e:
        log.error("Errore email: %s", e)
        return False

