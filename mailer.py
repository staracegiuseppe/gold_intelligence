# mailer.py — Gold Intelligence Email Reporter
# Invia report HTML via SMTP dopo ogni run scheduler

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Any, Optional

log = logging.getLogger("gold_mailer")


def _score_color(score: int) -> str:
    if score > 30:  return "#18B85A"
    if score < -30: return "#E03838"
    return "#C8A020"

def _bias_label(bias: Optional[str]) -> str:
    if bias == "bullish": return "▲ BULLISH"
    if bias == "bearish": return "▼ BEARISH"
    return "→ NEUTRALE"

def _bias_color(bias: Optional[str]) -> str:
    if bias == "bullish": return "#18B85A"
    if bias == "bearish": return "#E03838"
    return "#C8A020"


def build_html_report(results: List[Dict], run_ts: str, next_ts: str) -> str:
    """Genera il corpo HTML del report email."""

    # Sezione asset cards
    cards_html = ""
    for r in results:
        score     = r.get("score", 0)
        col       = _score_color(score)
        analysis  = r.get("analysis") or {}
        bias      = analysis.get("bias")
        conf      = analysis.get("confidence", "—")
        summary   = analysis.get("summary", "")
        drivers   = analysis.get("drivers", [])
        risks     = analysis.get("risk_factors", [])
        has_ai    = bool(analysis and not analysis.get("_skipped"))
        bar_w     = min(100, abs(score))

        drivers_html = "".join(f'<li style="color:#18B85A;font-size:12px;margin-bottom:3px">+ {d}</li>' for d in drivers[:3])
        risks_html   = "".join(f'<li style="color:#E03838;font-size:12px;margin-bottom:3px">! {r}</li>'  for r in risks[:3])

        cards_html += f"""
        <div style="background:#0F1520;border:1px solid #1E2D45;border-left:4px solid {col};
                    border-radius:6px;padding:16px;margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">
            <div>
              <span style="font-size:16px;font-weight:700;color:#D0DFF8">{r['symbol']}</span>
              <span style="font-size:11px;color:#6880A0;margin-left:8px">{r['name']}</span>
              <span style="font-size:10px;color:#6880A0;margin-left:6px;background:#192030;
                           padding:2px 6px;border-radius:3px">{r.get('type','').upper()} · {r.get('sector','').upper()}</span>
            </div>
            <div style="text-align:right">
              <span style="font-size:24px;font-weight:700;color:{col};font-family:monospace">
                {'+' if score>0 else ''}{score}
              </span>
              <span style="font-size:10px;color:#6880A0">/100</span>
            </div>
          </div>

          <!-- Score bar -->
          <div style="height:4px;background:#192030;border-radius:2px;margin-bottom:12px">
            <div style="height:100%;width:{bar_w}%;background:{col};border-radius:2px"></div>
          </div>

          {"<!-- AI analysis -->" + f"""
          <div style="background:#07090D;border-radius:4px;padding:12px;margin-bottom:10px">
            <div style="display:flex;gap:10px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
              <span style="background:{_bias_color(bias)}18;color:{_bias_color(bias)};
                           border:1px solid {_bias_color(bias)}44;padding:3px 10px;
                           border-radius:3px;font-size:11px;font-weight:700">
                {_bias_label(bias)}
              </span>
              <span style="color:#6880A0;font-size:11px">Confidence: 
                <b style="color:{_bias_color(bias)}">{conf}%</b>
              </span>
              <span style="color:#6880A0;font-size:11px">{analysis.get('time_horizon','').upper()}</span>
            </div>
            <p style="font-size:12px;color:#B0C4DE;line-height:1.7;margin:0 0 8px;
                      border-left:2px solid #C8A020;padding-left:10px">{summary}</p>
            {"<ul style='margin:6px 0 0 0;padding:0 0 0 16px'>" + drivers_html + "</ul>" if drivers_html else ""}
          </div>""" if has_ai else
          '<p style="font-size:11px;color:#6880A044;margin:0">Score sotto soglia — nessuna analisi AI</p>'}

        </div>"""

    # Separa top opportunità
    top_bull = [r for r in results if r.get("score",0) > 30][:5]
    top_bear = [r for r in results if r.get("score",0) < -30][:3]

    top_html = ""
    if top_bull:
        top_html += '<div style="margin-bottom:6px;font-size:11px;color:#18B85A;font-weight:600">▲ OPPORTUNITÀ RIALZISTE</div>'
        for r in top_bull:
            top_html += f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #192030;font-size:12px"><span style="color:#D0DFF8">{r["symbol"]} — {r["name"][:25]}</span><span style="color:#18B85A;font-family:monospace;font-weight:700">+{r["score"]}</span></div>'
    if top_bear:
        top_html += '<div style="margin-top:10px;margin-bottom:6px;font-size:11px;color:#E03838;font-weight:600">▼ SEGNALI RIBASSISTI</div>'
        for r in top_bear:
            top_html += f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #192030;font-size:12px"><span style="color:#D0DFF8">{r["symbol"]} — {r["name"][:25]}</span><span style="color:#E03838;font-family:monospace;font-weight:700">{r["score"]}</span></div>'

    run_dt  = run_ts[:19].replace("T"," ") if run_ts else "—"
    next_dt = next_ts[:19].replace("T"," ") if next_ts else "—"

    return f"""<!DOCTYPE html>
<html lang="it">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Gold Intelligence Report</title></head>
<body style="margin:0;padding:0;background:#07090D;font-family:'Segoe UI',system-ui,sans-serif">
<div style="max-width:680px;margin:0 auto;padding:20px">

  <!-- HEADER -->
  <div style="background:linear-gradient(135deg,#0B0E16,#111827);border:1px solid #1E2D45;
              border-radius:8px;padding:24px;margin-bottom:20px;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#C8A020;letter-spacing:.04em">◈ GOLD INTELLIGENCE</div>
    <div style="font-size:12px;color:#6880A0;margin-top:4px;letter-spacing:.1em">
      REPORT AUTOMATICO · SMART MONEY + CLAUDE AI
    </div>
    <div style="margin-top:12px;display:flex;justify-content:center;gap:20px;font-size:11px;color:#6880A0">
      <span>Generato: <b style="color:#D0DFF8">{run_dt}</b></span>
      <span>Prossimo: <b style="color:#D0DFF8">{next_dt}</b></span>
      <span>Asset: <b style="color:#C8A020">{len(results)}</b></span>
    </div>
  </div>

  <!-- TOP SUMMARY -->
  {"<div style='background:#0B0E16;border:1px solid #1E2D45;border-radius:6px;padding:16px;margin-bottom:20px'><div style='font-size:11px;color:#6880A0;letter-spacing:.1em;text-transform:uppercase;margin-bottom:12px'>⊕ Riepilogo Opportunità</div>" + top_html + "</div>" if top_html else ""}

  <!-- ASSET DETAILS -->
  <div style="font-size:11px;color:#6880A0;letter-spacing:.1em;text-transform:uppercase;margin-bottom:10px">
    ◈ Analisi Completa Asset
  </div>
  {cards_html}

  <!-- FOOTER -->
  <div style="text-align:center;padding:16px;font-size:10px;color:#6880A044;margin-top:8px">
    Gold Intelligence Add-on · Home Assistant · Report automatico ogni ora<br>
    I dati di mercato sono simulati · Non costituisce consulenza finanziaria
  </div>
</div>
</body>
</html>"""


def send_report(
    results:  List[Dict],
    run_ts:   str,
    next_ts:  str,
    cfg:      Dict[str, Any],
) -> bool:
    """
    Invia il report via email.
    Restituisce True se inviato con successo, False altrimenti.
    """
    if not cfg.get("email_enabled"):
        return False

    required = ["email_to","email_from","smtp_host","smtp_user","smtp_password"]
    missing  = [k for k in required if not cfg.get(k)]
    if missing:
        log.warning(f"Email skippata — campi mancanti: {missing}")
        return False

    # Filtra solo asset con segnale >= email_min_score
    min_score = int(cfg.get("email_min_score", 40))
    strong    = [r for r in results if abs(r.get("score",0)) >= min_score]
    if not strong and results:
        log.info(f"Nessun asset con |score| >= {min_score} — email non inviata")
        return False

    # Usa tutti i risultati nel corpo ma evidenzia i forti
    html_body = build_html_report(results, run_ts, next_ts)

    n_bull = len([r for r in results if r.get("score",0) > min_score])
    n_bear = len([r for r in results if r.get("score",0) < -min_score])
    subject_parts = []
    if n_bull: subject_parts.append(f"▲ {n_bull} BULL")
    if n_bear: subject_parts.append(f"▼ {n_bear} BEAR")
    subject = f"◈ Gold Intelligence — {', '.join(subject_parts) or 'Nessun segnale forte'} · {run_ts[:10]}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["email_from"]
    msg["To"]      = cfg["email_to"]
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    host     = cfg.get("smtp_host","smtp.gmail.com")
    port     = int(cfg.get("smtp_port", 587))
    use_tls  = cfg.get("smtp_tls", True)
    user     = cfg["smtp_user"]
    password = cfg["smtp_password"]

    try:
        log.info(f"Invio email a {cfg['email_to']} via {host}:{port} (tls={use_tls})")
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=15)
            server.ehlo()
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        server.login(user, password)
        server.sendmail(cfg["email_from"], cfg["email_to"], msg.as_string())
        server.quit()
        log.info("✓ Email inviata con successo")
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("✗ Autenticazione SMTP fallita — verifica user/password")
        return False
    except smtplib.SMTPException as e:
        log.error(f"✗ Errore SMTP: {e}")
        return False
    except Exception as e:
        log.error(f"✗ Errore invio email: {e}")
        return False
