"""Alert delivery for the data detector. Channels are opt-in through env vars.

  Slack:  ALERT_SLACK_WEBHOOK_URL
  Email:  ALERT_EMAIL_TO (comma-separated), SMTP_HOST, SMTP_PORT (587),
          SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_FROM (defaults to SMTP_USER)

With no channel configured the message is logged only; the daily flow check
still records quality events and fails its Airflow task, so nothing is silent.
"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage

import requests

from etl.src.data_detector.common import logger


def configured_channels() -> list[str]:
    channels = []
    if os.getenv("ALERT_SLACK_WEBHOOK_URL"):
        channels.append("slack")
    if os.getenv("ALERT_EMAIL_TO") and os.getenv("SMTP_HOST"):
        channels.append("email")
    return channels


def _send_slack(subject: str, body: str) -> None:
    resp = requests.post(os.environ["ALERT_SLACK_WEBHOOK_URL"],
                         json={"text": f"*{subject}*\n{body}"}, timeout=10)
    resp.raise_for_status()


def _send_email(subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.getenv("ALERT_EMAIL_FROM") or os.getenv("SMTP_USER") or "ballistics@localhost"
    msg["To"] = os.environ["ALERT_EMAIL_TO"]
    msg.set_content(body)
    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.getenv("SMTP_PORT", "587")), timeout=20) as smtp:
        smtp.starttls()
        if os.getenv("SMTP_USER"):
            smtp.login(os.environ["SMTP_USER"], os.environ.get("SMTP_PASSWORD", ""))
        smtp.send_message(msg)


SENDERS = {"slack": _send_slack, "email": _send_email}


def send_alert(subject: str, body: str) -> list[str]:
    """Send through every configured channel; return the channels that succeeded."""
    sent = []
    channels = configured_channels()
    if not channels:
        logger.warning("No alert channel configured. %s\n%s", subject, body)
        print(f"[alert, no channel configured] {subject}\n{body}")
        return sent
    for name in channels:
        try:
            SENDERS[name](subject, body)
            sent.append(name)
        except Exception as exc:   # one broken channel must not hide the alert elsewhere
            logger.error("Alert via %s failed: %s", name, exc)
            print(f"[alert via {name} failed] {type(exc).__name__}: {exc}")
    return sent
