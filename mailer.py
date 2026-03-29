# mailer.py - Gold Intelligence Email Reporter
# HTML generato con join/concatenazione - zero f-string annidate

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Any

log = logging.getLogger("gold_mailer")


def _col(score):
    if score > 30:  return "#18B85A"
    if score < -30: return "#E03838"
    return "#C8A020"

def _bcol(bias):
    if bias == "bullish": return "#18B85A"
    if bias == "bearish": return "#E03838"
    return "#C8A020"

def _blbl(bias):
    if bias == "bullish": return "BULLISH"
    if bias == "bearish": return "BEARISH"
    return "NEUTRALE"


def _card(r):
    score  = r.get("score", 0)
    col    = _col(score)
    bw     = min(100, abs(score))
    an     = r.get("analysis") or {}
    bias   = an.get("bias", "neutral")
    conf   = an.get("confidence", 0)
    summ   = an.get("summary", "")
    drvs   = an.get("drivers", [])
    has_ai = bool(an and not an.get("_skipped") and summ)
    bc     = _bcol(bias)
    sym    = r.get("symbol", "")
    nm     = r.get("name", "")
    tp     = r.get("type", "").upper()
    sec    = r.get("sector", "").upper()
    sc_s   = ("+" if score > 0 else "") + str(score)
    th     = (an.get("time_horizon") or "").upper()

    parts = []
    parts.append('<div style="background:#0F1520;border:1px solid #1E2D45;border-left:4px solid ')
    parts.append(col)
    parts.append(';border-radius:6px;padding:16px;margin-bottom:12px">')

    # Header row
    parts.append('<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">')
    parts.append('<div><span style="font-size:16px;font-weight:700;color:#D0DFF8">')
    parts.append(sym)
    parts.append('</span><span style="font-size:11px;color:#6880A0;margin-left:8px">')
    parts.append(nm)
    parts.append('</span><span style="font-size:10px;color:#6880A0;margin-left:6px;background:#192030;padding:2px 6px;border-radius:3px">')
    parts.append(tp + " - " + sec)
    parts.append('</span></div>')
    parts.append('<div style="text-align:right"><span style="font-size:24px;font-weight:700;font-family:monospace;color:')
    parts.append(col)
    parts.append('">')
    parts.append(sc_s)
    parts.append('</span><span style="font-size:10px;color:#6880A0">/100</span></div>')
    parts.append('</div>')

    # Score bar
    parts.append('<div style="height:4px;background:#192030;border-radius:2px;margin-bottom:12px">')
    parts.append('<div style="height:100%;width:')
    parts.append(str(bw))
    parts.append('%;background:')
    parts.append(col)
    parts.append(';border-radius:2px"></div></div>')

    # AI section
    if has_ai:
        parts.append('<div style="background:#07090D;border-radius:4px;padding:12px;margin-bottom:10px">')
        parts.append('<div style="margin-bottom:8px">')
        parts.append('<span style="padding:3px 10px;border-radius:3px;font-size:11px;font-weight:700;background:')
        parts.append(bc)
        parts.append('22;color:')
        parts.append(bc)
        parts.append(';border:1px solid ')
        parts.append(bc)
        parts.append('66">')
        parts.append(_blbl(bias))
        parts.append('</span> ')
        parts.append('<span style="color:#6880A0;font-size:11px">Confidence: <b style="color:')
        parts.append(bc)
        parts.append('">')
        parts.append(str(conf))
        parts.append('%</b></span>')
        if th:
            parts.append(' <span style="color:#6880A0;font-size:11px">')
            parts.append(th)
            parts.append('</span>')
        parts.append('</div>')
        parts.append('<p style="font-size:12px;color:#B0C4DE;line-height:1.7;margin:0 0 8px;border-left:2px solid #C8A020;padding-left:10px">')
        parts.append(summ)
        parts.append('</p>')
        if drvs:
            parts.append('<ul style="margin:4px 0 0 0;padding:0 0 0 16px">')
            for d in drvs[:3]:
                parts.append('<li style="color:#18B85A;font-size:12px;margin-bottom:2px">+ ')
                parts.append(d)
                parts.append('</li>')
            parts.append('</ul>')
        parts.append('</div>')
    else:
        parts.append('<p style="font-size:11px;color:#888;margin:0">Score sotto soglia - nessuna analisi AI</p>')

    parts.append('</div>')
    return "".join(parts)


def _summary_block(results, min_score):
    bulls = [r for r in results if r.get("score", 0) >= min_score]
    bears = [r for r in results if r.get("score", 0) <= -min_score]
    if not bulls and not bears:
        return ""
    rows = []
    if bulls:
        rows.append('<div style="margin-bottom:6px;font-size:11px;color:#18B85A;font-weight:600">OPPORTUNITA RIALZISTE</div>')
        for r in bulls:
            rows.append('<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #192030;font-size:12px">')
            rows.append('<span style="color:#D0DFF8">' + r["symbol"] + " - " + r["name"][:25] + '</span>')
            rows.append('<span style="color:#18B85A;font-family:monospace;font-weight:700">+' + str(r["score"]) + '</span>')
            rows.append('</div>')
    if bears:
        rows.append('<div style="margin-top:10px;margin-bottom:6px;font-size:11px;color:#E03838;font-weight:600">SEGNALI RIBASSISTI</div>')
        for r in bears:
            rows.append('<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #192030;font-size:12px">')
            rows.append('<span style="color:#D0DFF8">' + r["symbol"] + " - " + r["name"][:25] + '</span>')
            rows.append('<span style="color:#E03838;font-family:monospace;font-weight:700">' + str(r["score"]) + '</span>')
            rows.append('</div>')
    return (
        '<div style="background:#0B0E16;border:1px solid #1E2D45;border-radius:6px;padding:16px;margin-bottom:20px">'
        '<div style="font-size:11px;color:#6880A0;letter-spacing:.1em;text-transform:uppercase;margin-bottom:12px">Riepilogo</div>'
        + "".join(rows)
        + '</div>'
    )


def build_html_report(results, run_ts, next_ts):
    run_dt  = (run_ts[:19].replace("T", " ")) if run_ts else "---"
    next_dt = (next_ts[:19].replace("T", " ")) if next_ts else "---"
    n       = len(results)
    min_sc  = 40
    cards   = "".join(_card(r) for r in results)
    summary = _summary_block(results, min_sc)

    parts = [
        "<!DOCTYPE html><html lang=\"it\"><head><meta charset=\"UTF-8\"/>",
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\"/>",
        "<title>Gold Intelligence Report</title></head>",
        "<body style=\"margin:0;padding:0;background:#07090D;font-family:Segoe UI,system-ui,sans-serif\">",
        "<div style=\"max-width:680px;margin:0 auto;padding:20px\">",

        # Header
        "<div style=\"background:linear-gradient(135deg,#0B0E16,#111827);border:1px solid #1E2D45;",
        "border-radius:8px;padding:24px;margin-bottom:20px;text-align:center\">",
        "<div style=\"font-size:26px;font-weight:700;color:#C8A020\">Gold Intelligence</div>",
        "<div style=\"font-size:12px;color:#6880A0;margin-top:4px\">REPORT AUTOMATICO - SMART MONEY + CLAUDE AI</div>",
        "<div style=\"margin-top:12px;font-size:11px;color:#6880A0\">",
        "Generato: <b style=\"color:#D0DFF8\">" + run_dt + "</b>",
        "&nbsp;&nbsp;Prossimo: <b style=\"color:#D0DFF8\">" + next_dt + "</b>",
        "&nbsp;&nbsp;Asset: <b style=\"color:#C8A020\">" + str(n) + "</b>",
        "</div></div>",

        summary,

        "<div style=\"font-size:11px;color:#6880A0;letter-spacing:.1em;text-transform:uppercase;margin-bottom:10px\">",
        "Analisi Completa Asset</div>",
        cards,

        "<div style=\"text-align:center;padding:16px;font-size:10px;color:#555;margin-top:8px\">",
        "Gold Intelligence Add-on - Home Assistant<br>",
        "Dati simulati - Non costituisce consulenza finanziaria",
        "</div>",
        "</div></body></html>",
    ]
    return "".join(parts)


def send_report(results, run_ts, next_ts, cfg):
    if not cfg.get("email_enabled"):
        return False
    required = ["email_to", "email_from", "smtp_host", "smtp_user", "smtp_password"]
    missing  = [k for k in required if not cfg.get(k)]
    if missing:
        log.warning("Email skip - mancanti: " + str(missing))
        return False
    min_score = int(cfg.get("email_min_score", 40))
    strong    = [r for r in results if abs(r.get("score", 0)) >= min_score]
    if not strong:
        log.info("Nessun segnale forte - email non inviata")
        return False

    html_body = build_html_report(results, run_ts, next_ts)
    n_bull  = len([r for r in results if r.get("score", 0) >= min_score])
    n_bear  = len([r for r in results if r.get("score", 0) <= -min_score])
    parts   = []
    if n_bull: parts.append(str(n_bull) + " BULL")
    if n_bear: parts.append(str(n_bear) + " BEAR")
    subject = "Gold Intelligence - " + (", ".join(parts) or "Nessun segnale") + " - " + (run_ts[:10] if run_ts else "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["email_from"]
    msg["To"]      = cfg["email_to"]
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    host = cfg.get("smtp_host", "smtp.gmail.com")
    port = int(cfg.get("smtp_port", 587))
    tls  = cfg.get("smtp_tls", True)
    user = cfg["smtp_user"]
    pw   = cfg["smtp_password"]
    try:
        log.info("Invio email a " + cfg["email_to"] + " via " + host)
        if tls:
            srv = smtplib.SMTP(host, port, timeout=15)
            srv.ehlo(); srv.starttls()
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
        log.error("Errore email: " + str(e))
        return False
