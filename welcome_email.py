#!/usr/bin/env python3
"""
Welcome Email Sender

Sends HTML welcome emails to leaders entering Onboarding Setup.
Includes hyperlinked resources: Calendly training, Notion checklist,
onboarding quiz, and the Kodely Leader App.

Can be called standalone or imported by onboarding_digest.py.

Usage:
  python welcome_email.py --dry-run   # preview without sending
  python welcome_email.py             # send to all eligible leaders
"""

import argparse
import json
import logging
import os
import re
import smtplib
import ssl
import time
from datetime import datetime, timezone

import certifi
import httpx
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config

os.environ.setdefault("SSL_CERT_FILE", certifi.where())
ssl._create_default_https_context = lambda: ssl.create_default_context(
    cafile=certifi.where()
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

NOTION_BASE = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {config.NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.WELCOME_EMAIL_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.WELCOME_EMAIL_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------

def _get_property_value(page: dict, prop_name: str) -> str:
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    prop_type = prop.get("type", "")
    if prop_type == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if prop_type == "date":
        dt = prop.get("date")
        return dt.get("start", "") if dt else ""
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in parts)
    if prop_type == "email":
        return prop.get("email", "") or ""
    if prop_type == "rich_text":
        parts = prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_leader_name(page: dict) -> str:
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_email(page: dict) -> str:
    props = page.get("properties", {})
    email_prop = props.get("Email", {})
    if email_prop.get("type") == "email" and email_prop.get("email"):
        return email_prop["email"]
    if email_prop.get("type") == "rich_text":
        parts = email_prop.get("rich_text", [])
        val = "".join(t.get("plain_text", "") for t in parts).strip()
        if val:
            return val
    # Fallback: extract email embedded in the title field
    name = _get_leader_name(page)
    match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', name)
    if match:
        return match.group(0)
    return ""


def query_needs_welcome_email() -> list[dict]:
    """Query leaders in Onboarding Setup who haven't received the welcome email."""
    body = {
        "filter": {
            "and": [
                {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                {
                    "or": [
                        {"property": config.OB_ONBOARDING_EMAIL_PROPERTY, "status": {"is_empty": True}},
                        {"property": config.OB_ONBOARDING_EMAIL_PROPERTY, "status": {"equals": "Not Sent"}},
                    ],
                },
            ],
        },
        "page_size": 100,
    }
    resp = httpx.post(
        f"{NOTION_BASE}/databases/{config.ONBOARDING_DB_ID}/query",
        headers=NOTION_HEADERS,
        json=body,
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Notion query failed %s: %s", resp.status_code, resp.text)
        return []
    return resp.json().get("results", [])


def _mark_email_sent(page_id: str) -> bool:
    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {config.OB_ONBOARDING_EMAIL_PROPERTY: {"status": {"name": "Sent"}}}},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Failed to mark email sent for %s: %s %s", page_id, resp.status_code, resp.text)
        return False
    return True


# ---------------------------------------------------------------------------
# HTML email template
# ---------------------------------------------------------------------------

def build_welcome_html(leader_name: str, start_date: str, region: str) -> str:
    """Build the HTML welcome email body with hyperlinked resources."""
    first_name = leader_name.split()[0] if leader_name.strip() else leader_name

    calendly_link = config.WELCOME_CALENDLY_LINK
    notion_link = config.WELCOME_NOTION_CHECKLIST_LINK
    quiz_link = config.WELCOME_QUIZ_LINK
    app_link = config.WELCOME_APP_LINK
    android_link = config.WELCOME_ANDROID_LINK

    start_display = start_date if start_date else "TBD"

    # --- Build dynamic sections ---
    # 1. Schedule Training
    training_section = ""
    if calendly_link:
        training_section = f"""
  <h3 style="color:#1a1a2e;margin:24px 0 8px;">1. Schedule Your Training</h3>
  <p style="margin:4px 0 12px;">
    <a href="{calendly_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">
      Click here to schedule your Training Call &amp; Dress Rehearsal
    </a>
  </p>
"""

    # 2. Onboarding Checklist
    checklist_section = ""
    if notion_link:
        checklist_section = f"""
  <h3 style="color:#1a1a2e;margin:24px 0 8px;">2. New Hire Onboarding Checklist</h3>
  <p style="margin:4px 0 12px;">
    <a href="{notion_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">
      View Your Onboarding Checklist
    </a><br>
    This is where you'll find information on Gusto, the Kodely App, and compliance requirements.
  </p>
"""

    # 3. Kodely University / LearnDash
    learndash_section = """
  <h3 style="color:#1a1a2e;margin:24px 0 8px;">3. Kodely University</h3>
  <p style="margin:4px 0 4px;">
    Log into <a href="https://learn.kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">learn.kodely.io</a>.
    Use the email address we have on file for you.
  </p>
  <p style="margin:4px 0 4px;">
    Prepare only the first lesson you are assigned to teach.
  </p>
  <p style="margin:4px 0 12px;">
    Review and complete Kodely University which can be found at
    <a href="https://learn.kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">learn.kodely.io</a>.
  </p>
"""

    # 4. Download the Kodely Leader App
    app_section = ""
    app_links = []
    if app_link:
        app_links.append(
            f'<a href="{app_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">Download for iPhone (App Store)</a>'
        )
    if android_link:
        app_links.append(
            f'<a href="{android_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">Download for Android (Google Play)</a>'
        )
    if app_links:
        links_html = "<br>".join(app_links)
        app_section = f"""
  <h3 style="color:#1a1a2e;margin:24px 0 8px;">4. Download the Kodely Leader App</h3>
  <p style="margin:4px 0 12px;">
    {links_html}
  </p>
"""

    # 5. Onboarding Quiz (optional)
    quiz_section = ""
    if quiz_link:
        quiz_section = f"""
  <h3 style="color:#1a1a2e;margin:24px 0 8px;">5. Onboarding Quiz</h3>
  <p style="margin:4px 0 12px;">
    <a href="{quiz_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">
      Take the Onboarding Quiz
    </a> &mdash; Test your knowledge before your first session.
  </p>
"""

    html = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,Helvetica,sans-serif;max-width:600px;margin:0 auto;color:#1a1a2e;">

<div style="background:#1a1a2e;padding:24px 28px;border-radius:8px 8px 0 0;">
  <h1 style="color:#ffffff;margin:0;font-size:22px;">Welcome to Kodely!</h1>
</div>

<div style="padding:24px 28px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;">

  <p>Hi {first_name},</p>

  <p>Congratulations on being matched as a Kodely Leader! We're excited to have you
  on the team. Below you'll find everything you need to get started.</p>

  <table style="width:100%;border-collapse:collapse;margin:16px 0;">
    <tr>
      <td style="padding:8px 12px;background:#f3f4f6;font-weight:600;width:120px;">Region</td>
      <td style="padding:8px 12px;background:#f3f4f6;">{region}</td>
    </tr>
    <tr>
      <td style="padding:8px 12px;font-weight:600;">Start Date</td>
      <td style="padding:8px 12px;">{start_display}</td>
    </tr>
  </table>

  <hr style="border:none;border-top:1px solid #e5e7eb;margin:20px 0;">
{training_section}{checklist_section}{learndash_section}{app_section}{quiz_section}
  <div style="background:#fef3c7;border-left:4px solid #f59e0b;padding:12px 16px;margin:20px 0;border-radius:4px;">
    <strong>Important:</strong> Please complete all onboarding steps and schedule your
    training call before your start date ({start_display}).
  </div>

  <h3 style="color:#1a1a2e;margin:24px 0 8px;">Questions or Support</h3>
  <p>For all questions, please contact
    <a href="mailto:talent@kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">talent@kodely.io</a>.
  </p>

  <p style="margin-top:24px;">Best,<br>
  <strong>The Kodely Team</strong></p>

</div>

<p style="color:#999;font-size:11px;text-align:center;margin-top:16px;">
  This is an automated onboarding email from Kodely.
</p>

</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Returning Leader welcome email
# ---------------------------------------------------------------------------

def build_returning_welcome_html(leader_name: str, start_date: str, region: str) -> str:
    """Build the HTML welcome-back email for returning leaders."""
    first_name = leader_name.split()[0] if leader_name.strip() else leader_name
    start_display = start_date if start_date else "TBD"

    returning_calendly = config.WELCOME_RETURNING_CALENDLY_LINK
    app_link = config.WELCOME_APP_LINK
    android_link = config.WELCOME_ANDROID_LINK

    # App links
    app_links_html = ""
    parts = []
    if app_link:
        parts.append(
            f'<a href="{app_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">iPhone (App Store)</a>'
        )
    if android_link:
        parts.append(
            f'<a href="{android_link}" style="color:#2563eb;text-decoration:none;font-weight:600;">Android (Google Play)</a>'
        )
    if parts:
        app_links_html = " &nbsp;|&nbsp; ".join(parts)

    html = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,Helvetica,sans-serif;max-width:600px;margin:0 auto;color:#1a1a2e;">

<div style="background:#1a1a2e;padding:24px 28px;border-radius:8px 8px 0 0;">
  <h1 style="color:#ffffff;margin:0;font-size:22px;">Welcome Back to Kodely!</h1>
</div>

<div style="padding:24px 28px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;">

  <p>Hi {first_name},</p>

  <p>Welcome back! We're excited to have you leading another Kodely session.</p>

  <p>Please take a moment to confirm that everything is set up for your upcoming workshop:</p>

  <hr style="border:none;border-top:1px solid #e5e7eb;margin:20px 0;">

  <h3 style="color:#1a1a2e;margin:24px 0 8px;">1. Check Your Kodely Leader App</h3>
  <p style="margin:4px 0 12px;">
    Make sure your new session appears in the app. If you don't see it, reach out to
    <a href="mailto:support@kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">support@kodely.io</a>.<br>
    {app_links_html}
  </p>

  <h3 style="color:#1a1a2e;margin:24px 0 8px;">2. Review Your New Lessons</h3>
  <p style="margin:4px 0 12px;">
    Head to <a href="https://learn.kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">learn.kodely.io</a>
    and review the updated lesson plan for your upcoming class.
    A new lesson plan will be added to your account &mdash; be sure to review it before Day 1!
  </p>

  <h3 style="color:#1a1a2e;margin:24px 0 8px;">3. Schedule Training</h3>
  <p style="margin:4px 0 12px;">
    <a href="{returning_calendly}" style="color:#2563eb;text-decoration:none;font-weight:600;">
      Click here to schedule your Returning Leaders Training Check-In
    </a>
  </p>

  <div style="background:#fef3c7;border-left:4px solid #f59e0b;padding:12px 16px;margin:20px 0;border-radius:4px;">
    <strong>Important:</strong> Training is required before you can begin at your school.
  </div>

  <p>That's it! Let us know if you have any questions or need support &mdash; we're here to help.</p>

  <h3 style="color:#1a1a2e;margin:24px 0 8px;">Questions or Support</h3>
  <p>For all questions, please contact
    <a href="mailto:talent@kodely.io" style="color:#2563eb;text-decoration:none;font-weight:600;">talent@kodely.io</a>.
  </p>

  <p style="margin-top:24px;">Best,<br>
  <strong>The Kodely Team</strong></p>

</div>

<p style="color:#999;font-size:11px;text-align:center;margin-top:16px;">
  This is an automated onboarding email from Kodely.
</p>

</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Send email (reuses existing SMTP pattern from email_digest.py)
# ---------------------------------------------------------------------------

def send_welcome_email(to_email: str, leader_name: str, html: str, subject: str | None = None) -> bool:
    """Send the welcome email via SMTP. Returns True on success."""
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    first_name = leader_name.split()[0] if leader_name.strip() else leader_name
    if subject is None:
        subject = f"Welcome to Kodely, {first_name}! - Onboarding Instructions"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_FROM
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html"))

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
            server.starttls(context=context)
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.EMAIL_FROM, [to_email], msg.as_string())
        return True
    except Exception:
        log.exception("Failed to send welcome email to %s", to_email)
        return False


# ---------------------------------------------------------------------------
# Slack helper
# ---------------------------------------------------------------------------

def _post_to_slack(slack: SlackClient, message: str) -> None:
    for attempt in range(3):
        try:
            slack.chat_postMessage(channel=config.SLACK_ONBOARDING_CHANNEL, text=message)
            return
        except SlackApiError as e:
            if e.response["error"] == "ratelimited" and attempt < 2:
                wait = int(e.response.headers.get("Retry-After", 5))
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Public API (called from onboarding_digest.py hooks)
# ---------------------------------------------------------------------------

def send_welcome_for_page(
    page: dict,
    slack: SlackClient | None = None,
    dry_run: bool = False,
    form_emails: dict[str, str] | None = None,
) -> bool:
    """Send welcome email for a single Notion page. Returns True if sent."""
    page_id = page.get("id", "")
    name = _get_leader_name(page)
    email = _get_email(page)
    if not email and form_emails:
        from checkr_sync import _resolve_email
        email = _resolve_email(page, form_emails)
    start_date = _get_property_value(page, "Start Date")
    region = _get_property_value(page, "Region")

    if not email:
        log.warning("No email for %s — cannot send welcome email", name)
        return False

    # Detect returning leader via the "Returning Leader?" property
    returning_val = _get_property_value(page, "Returning Leader?")
    is_returning = returning_val.lower() == "yes" if returning_val else False

    if dry_run:
        label = "RETURNING LEADER" if is_returning else "NEW LEADER"
        print(f"--- DRY RUN: WELCOME EMAIL ({label}) ---")
        print(f"  To: {name} <{email}>")
        print(f"  Region: {region}, Start: {start_date}")
        print()
        return True

    if is_returning:
        html = build_returning_welcome_html(name, start_date, region)
        subject = f"Welcome Back, {name.split()[0]}! – Confirm Your Upcoming Kodely Session"
    else:
        html = build_welcome_html(name, start_date, region)
        subject = None  # use default

    if not send_welcome_email(email, name, html, subject=subject):
        return False

    _mark_email_sent(page_id)
    log.info("Welcome email sent to %s (%s) [%s]", name, email, "returning" if is_returning else "new")

    if slack:
        label = "RETURNING LEADER" if is_returning else ""
        msg = (
            f":email: WELCOME EMAIL SENT {label}\n\n"
            f"*Leader:* {name}\n"
            f"*Email:* {email}\n\n"
            f"{'Welcome back' if is_returning else 'Onboarding welcome'} email has been delivered."
        )
        try:
            _post_to_slack(slack, msg)
        except Exception:
            log.exception("Failed to post welcome email alert for %s", name)

    return True


# ---------------------------------------------------------------------------
# Main (standalone mode)
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Welcome Email Sender")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    args = parser.parse_args()

    slack = None
    if not args.dry_run:
        slack = SlackClient(token=config.SLACK_BOT_TOKEN)

    # Load form emails as fallback for leaders without Email property
    form_emails = {}
    try:
        from checkr_sync import load_form_emails
        form_emails = load_form_emails()
    except Exception:
        log.warning("Could not load form emails — will rely on Notion Email property only")

    state = load_state()
    leaders = query_needs_welcome_email()
    log.info("Found %d leader(s) needing welcome email", len(leaders))

    sent = 0
    for page in leaders:
        page_id = page.get("id", "")
        if state.get(f"sent_{page_id}"):
            continue

        if send_welcome_for_page(page, slack=slack, dry_run=args.dry_run, form_emails=form_emails):
            state[f"sent_{page_id}"] = datetime.now(timezone.utc).isoformat()
            sent += 1

    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.WELCOME_EMAIL_STATE_PATH)

    log.info("Done. %d email(s) sent%s.", sent, " (dry run)" if args.dry_run else "")


if __name__ == "__main__":
    main()
