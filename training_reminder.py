#!/usr/bin/env python3
"""
Training Reminder Report

Queries Notion for leaders with no Trainer Assigned, groups them by start date
(this week / next week / overdue), posts a summary report to #ops-onboarding,
and DMs each leader a reminder to book their Calendly training.

Usage:
  python training_reminder.py --dry-run    # print everything, post nothing
  python training_reminder.py              # post report + send DMs
"""

import argparse
import json
import logging
import os
import re
import smtplib
import ssl
import time
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import certifi
import httpx
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from onboarding_tracker import post_to_slack, NOTION_BASE, NOTION_HEADERS
from onboarding_digest import (
    _get_leader_name,
    _get_property_value,
    _get_start_date,
)
from calendly_sync import (
    get_current_user,
    fetch_invitees,
    fetch_event_host,
    CALENDLY_BASE,
    CALENDLY_HEADERS as CAL_HEADERS,
)

os.environ.setdefault("SSL_CERT_FILE", certifi.where())
ssl._create_default_https_context = lambda: ssl.create_default_context(
    cafile=certifi.where()
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State file helpers (dedup DMs so leaders don't get spammed)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.TRAINING_REMINDER_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.TRAINING_REMINDER_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Query Notion for leaders needing training
# ---------------------------------------------------------------------------

def query_leaders_needing_training() -> list[dict]:
    """Query Notion for leaders in onboarding stages with no Trainer Assigned.

    Fetches leaders in Onboarding Setup or Training In Progress, then filters
    in Python for those missing a trainer.
    """
    notion_filter = {
        "and": [
            {
                "or": [
                    {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                    {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
                    {"property": "Readiness Status", "select": {"equals": "Matched"}},
                    {"property": "Readiness Status", "select": {"equals": "Background Check Pending"}},
                ],
            },
            {
                "property": "Trainer Assigned",
                "select": {"is_empty": True},
            },
        ],
    }

    results = []
    cursor = None

    while True:
        body: dict = {"filter": notion_filter, "page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = httpx.post(
            f"{NOTION_BASE}/databases/{config.ONBOARDING_DB_ID}/query",
            headers=NOTION_HEADERS,
            json=body,
            timeout=30,
        )
        if resp.status_code >= 400:
            log.error("Notion query failed %s: %s", resp.status_code, resp.text)
            break
        data = resp.json()
        results.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    log.info("Found %d leaders with no trainer assigned", len(results))
    return results


# ---------------------------------------------------------------------------
# Group leaders by time window
# ---------------------------------------------------------------------------

def group_by_week(leaders: list[dict]) -> dict[str, list[dict]]:
    """Group leaders into overdue, this_week, next_week, and later buckets.

    Each entry is a dict with name, start_date, email, page_id, page_url.
    """
    today = date.today()
    # Monday of this week
    week_start = today - timedelta(days=today.weekday())
    next_week_start = week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=7)

    buckets: dict[str, list[dict]] = {
        "overdue": [],
        "this_week": [],
        "next_week": [],
        "later": [],
    }

    for page in leaders:
        raw_name = _get_leader_name(page)
        if not raw_name:
            continue

        # Clean name: strip whitespace, remove embedded emails, collapse newlines
        email_in_name = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', raw_name)
        name = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', raw_name)
        name = re.sub(r'\s+', ' ', name).strip()

        start = _get_start_date(page)

        # Extract email: try Notion email property first, then embedded in name
        props = page.get("properties", {})
        email_prop = props.get("Email", {})
        email = ""
        if email_prop.get("type") == "email":
            email = email_prop.get("email", "") or ""
        if not email and email_in_name:
            email = email_in_name.group(0)

        entry = {
            "name": name,
            "start_date": start,
            "email": email,
            "page_id": page.get("id", ""),
            "page_url": page.get("url", ""),
            "status": _get_property_value(page, "Readiness Status"),
        }

        if start is None:
            buckets["later"].append(entry)
        elif start < today:
            buckets["overdue"].append(entry)
        elif start < next_week_start:
            buckets["this_week"].append(entry)
        elif start < next_week_end:
            buckets["next_week"].append(entry)
        else:
            buckets["later"].append(entry)

    # Sort each bucket by start date
    for key in buckets:
        buckets[key].sort(key=lambda e: e["start_date"] or date.max)

    return buckets


# ---------------------------------------------------------------------------
# Calendly: expedited / upcoming training events
# ---------------------------------------------------------------------------

def _fetch_calendly_events(org_uri: str, event_names: list[str],
                           min_start: datetime | None = None,
                           max_start: datetime | None = None) -> list[dict]:
    """Fetch Calendly events matching *event_names* within a time window."""
    now = datetime.now(timezone.utc)
    params: dict = {
        "organization": org_uri,
        "min_start_time": (min_start or now).isoformat(),
        "status": "active",
        "count": 100,
    }
    if max_start:
        params["max_start_time"] = max_start.isoformat()

    resp = httpx.get(
        f"{CALENDLY_BASE}/scheduled_events",
        headers=CAL_HEADERS,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()

    matched = []
    for event in resp.json().get("collection", []):
        name_lower = event.get("name", "").lower()
        if any(term in name_lower for term in event_names):
            matched.append(event)
    return matched


def fetch_expedited_events(org_uri: str) -> list[dict]:
    """Return upcoming expedited/feedback training events with invitee info."""
    events = _fetch_calendly_events(
        org_uri, config.CALENDLY_EXPEDITED_EVENT_NAMES,
    )
    results = []
    for event in events:
        trainer = fetch_event_host(event)
        invitees = fetch_invitees(event["uri"])
        start = event.get("start_time", "")
        for inv in invitees:
            results.append({
                "invitee_name": inv.get("name", "Unknown"),
                "trainer_name": trainer,
                "event_date": start,
                "event_name": event.get("name", ""),
            })
    return results


def fetch_upcoming_training_sessions(org_uri: str) -> list[dict]:
    """Return all training sessions (regular + expedited) in the next 7 days."""
    now = datetime.now(timezone.utc)
    events = _fetch_calendly_events(
        org_uri, config.CALENDLY_TRAINING_EVENT_NAMES,
        min_start=now, max_start=now + timedelta(days=7),
    )
    results = []
    for event in events:
        trainer = fetch_event_host(event)
        invitees = fetch_invitees(event["uri"])
        start = event.get("start_time", "")
        for inv in invitees:
            results.append({
                "invitee_name": inv.get("name", "Unknown"),
                "trainer_name": trainer,
                "event_date": start,
                "event_name": event.get("name", ""),
            })
    return results


def fetch_recently_assigned_trainers() -> list[dict]:
    """Query Notion for leaders who have a Trainer Assigned (last 7 days context)."""
    notion_filter = {
        "and": [
            {
                "or": [
                    {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                    {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
                ],
            },
            {
                "property": "Trainer Assigned",
                "select": {"is_not_empty": True},
            },
        ],
    }
    results = []
    cursor = None
    while True:
        body: dict = {"filter": notion_filter, "page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = httpx.post(
            f"{NOTION_BASE}/databases/{config.ONBOARDING_DB_ID}/query",
            headers=NOTION_HEADERS,
            json=body,
            timeout=30,
        )
        if resp.status_code >= 400:
            log.error("Notion query failed %s: %s", resp.status_code, resp.text)
            break
        data = resp.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    entries = []
    for page in results:
        name = _get_leader_name(page)
        if not name:
            continue
        trainer = _get_property_value(page, "Trainer Assigned")
        stage = _get_property_value(page, "Readiness Status")
        entries.append({"name": name, "trainer": trainer, "stage": stage})
    return entries


# ---------------------------------------------------------------------------
# Build Slack report (matches the screenshot format)
# ---------------------------------------------------------------------------

def build_report_message(buckets: dict[str, list[dict]]) -> str:
    """Build the Slack report message matching the screenshot format."""
    total = sum(len(v) for v in buckets.values())
    if total == 0:
        return ":white_check_mark: All leaders have a trainer assigned. No action needed."

    lines = [
        f"<!channel> Leaders need trainings.  Any takers?",
    ]

    def _format_entries(entries: list[dict]) -> list[str]:
        formatted = []
        for e in entries:
            date_str = e["start_date"].strftime("%B %-d, %Y") if e["start_date"] else "No start date"
            formatted.append(f">  {e['name']} \u2013 {date_str}")
        return formatted

    if buckets["overdue"]:
        lines.append("")
        lines.append("*No Trainer assigned (Overdue \u2014 already started)*")
        lines.extend(_format_entries(buckets["overdue"]))

    if buckets["this_week"]:
        lines.append("")
        lines.append("*No Trainer assigned (This week)*")
        lines.extend(_format_entries(buckets["this_week"]))

    if buckets["next_week"]:
        lines.append("")
        lines.append("*For next week* (_start dates_)")
        lines.extend(_format_entries(buckets["next_week"]))

    if buckets["later"]:
        lines.append("")
        lines.append("*Coming up*")
        lines.extend(_format_entries(buckets["later"]))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DM leaders a booking reminder via Slack
# ---------------------------------------------------------------------------

def _lookup_slack_user(slack: SlackClient, email: str) -> str | None:
    """Look up a Slack user ID by email. Returns None if not found."""
    try:
        resp = slack.users_lookupByEmail(email=email)
        if resp["ok"]:
            return resp["user"]["id"]
    except SlackApiError:
        log.debug("Could not find Slack user for %s", email)
    return None


def send_leader_reminders(
    buckets: dict[str, list[dict]],
    slack: SlackClient,
    state: dict,
    dry_run: bool = False,
) -> int:
    """DM leaders who haven't booked training yet.

    Only reminds leaders whose start date is within the next 2 weeks or overdue.
    Deduplicates so each leader gets at most one DM per week.
    """
    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send leader training reminders")
        return 0

    today = date.today()
    week_key = today.strftime("%Y-W%W")  # e.g. "2026-W06"
    reminded = 0

    # Only nudge overdue, this_week, and next_week leaders
    urgent_leaders = buckets["overdue"] + buckets["this_week"] + buckets["next_week"]

    for entry in urgent_leaders:
        email = entry.get("email", "").strip()
        if not email:
            log.debug("No email for %s — skipping DM", entry["name"])
            continue

        # Dedup: one DM per leader per week
        dedup_key = f"dm::{email}::{week_key}"
        if dedup_key in state:
            log.debug("Already reminded %s this week — skipping", entry["name"])
            continue

        start_str = entry["start_date"].strftime("%B %-d, %Y") if entry["start_date"] else "soon"

        dm_text = (
            f"Hi {entry['name'].split()[0]}! :wave:\n\n"
            f"Your Kodely program starts *{start_str}* and you don't have a training session booked yet.\n\n"
            f"Please book your training call here:\n"
            f":calendar: {config.CALENDLY_BOOKING_URL}\n\n"
            f"If you have any questions, reach out in #ops-onboarding. Thanks!"
        )

        if dry_run:
            print(f"--- DRY RUN: DM to {entry['name']} ({email}) ---")
            print(dm_text)
            print()
            reminded += 1
            continue

        user_id = _lookup_slack_user(slack, email)
        if not user_id:
            log.warning("Could not find Slack user for %s (%s) — skipping DM", entry["name"], email)
            continue

        try:
            slack.chat_postMessage(channel=user_id, text=dm_text)
            log.info("Sent training reminder DM to %s (%s)", entry["name"], email)
            state[dedup_key] = datetime.now(timezone.utc).isoformat()
            reminded += 1
        except SlackApiError:
            log.exception("Failed to DM %s (%s)", entry["name"], email)

    return reminded


# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

def _fmt_event_date(iso_str: str) -> str:
    """Format an ISO datetime string for display (e.g. 'Feb 13 3:30 PM')."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%b %-d %-I:%M %p")
    except (ValueError, AttributeError):
        return iso_str or "TBD"


def build_email_html(
    buckets: dict[str, list[dict]],
    expedited: list[dict],
    upcoming: list[dict],
    recently_assigned: list[dict],
) -> str:
    """Build the Daily Training Report HTML email."""
    today_str = date.today().strftime("%b %d, %Y")
    total_no_trainer = sum(len(v) for v in buckets.values())

    html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'></head><body style='font-family:Arial,sans-serif;max-width:700px;margin:auto;'>",
        f"<h2 style='background:#1a1a2e;color:#fff;padding:14px 18px;margin:0;border-radius:6px 6px 0 0;'>KODELY DAILY TRAINING REPORT &mdash; {today_str}</h2>",
    ]

    # --- Section 1: No Trainer Assigned ---
    if total_no_trainer:
        html_parts.append(f"<h3 style='margin:18px 0 8px;'>&#9888;&#65039; NO TRAINER ASSIGNED ({total_no_trainer} leader{'s' if total_no_trainer != 1 else ''})</h3>")
        html_parts.append("<table style='border-collapse:collapse;width:100%;' border='1' cellpadding='6' cellspacing='0'>")
        html_parts.append("<tr style='background:#eee;'><th>Leader</th><th>Start Date</th><th>Status</th></tr>")

        status_labels = {
            "overdue": ("Overdue", "#ffcccc", "&#128308; "),
            "this_week": ("This week", "#fff3cd", "&#128993; "),
            "next_week": ("Next week", "#ffffff", ""),
            "later": ("Coming up", "#ffffff", ""),
        }
        for bucket_key in ("overdue", "this_week", "next_week", "later"):
            label, bg, icon = status_labels[bucket_key]
            for e in buckets[bucket_key]:
                d = e["start_date"].strftime("%b %-d") if e["start_date"] else "No date"
                html_parts.append(
                    f"<tr style='background:{bg};'>"
                    f"<td>{icon}{e['name']}</td>"
                    f"<td>{d}</td>"
                    f"<td>{label}</td></tr>"
                )
        html_parts.append("</table>")
    else:
        html_parts.append("<p style='color:green;font-weight:bold;'>&#9989; All leaders have a trainer assigned.</p>")

    # --- Section 2: Expedited / Feedback Training ---
    html_parts.append("<h3 style='margin:18px 0 8px;'>&#128203; EXPEDITED / FEEDBACK TRAINING</h3>")
    if expedited:
        html_parts.append("<table style='border-collapse:collapse;width:100%;' border='1' cellpadding='6' cellspacing='0'>")
        html_parts.append("<tr style='background:#eee;'><th>Leader</th><th>Trainer</th><th>Date</th></tr>")
        for e in expedited:
            html_parts.append(
                f"<tr><td>{e['invitee_name']}</td>"
                f"<td>{e['trainer_name']}</td>"
                f"<td>{_fmt_event_date(e['event_date'])}</td></tr>"
            )
        html_parts.append("</table>")
    else:
        html_parts.append("<p style='color:#888;'>No expedited/feedback training sessions found.</p>")

    # --- Section 3: Upcoming Training (next 7 days) ---
    html_parts.append("<h3 style='margin:18px 0 8px;'>&#128197; UPCOMING TRAINING (next 7 days)</h3>")
    if upcoming:
        html_parts.append("<table style='border-collapse:collapse;width:100%;' border='1' cellpadding='6' cellspacing='0'>")
        html_parts.append("<tr style='background:#eee;'><th>Leader</th><th>Trainer</th><th>Date</th></tr>")
        for e in upcoming:
            html_parts.append(
                f"<tr><td>{e['invitee_name']}</td>"
                f"<td>{e['trainer_name']}</td>"
                f"<td>{_fmt_event_date(e['event_date'])}</td></tr>"
            )
        html_parts.append("</table>")
    else:
        html_parts.append("<p style='color:#888;'>No upcoming training sessions in the next 7 days.</p>")

    # --- Section 4: Recently Assigned Trainers ---
    html_parts.append("<h3 style='margin:18px 0 8px;'>&#9989; RECENTLY ASSIGNED TRAINERS</h3>")
    if recently_assigned:
        html_parts.append("<table style='border-collapse:collapse;width:100%;' border='1' cellpadding='6' cellspacing='0'>")
        html_parts.append("<tr style='background:#eee;'><th>Leader</th><th>Trainer</th><th>Pipeline Stage</th></tr>")
        for e in recently_assigned:
            html_parts.append(
                f"<tr><td>{e['name']}</td>"
                f"<td>{e['trainer']}</td>"
                f"<td>{e['stage']}</td></tr>"
            )
        html_parts.append("</table>")
    else:
        html_parts.append("<p style='color:#888;'>No recently assigned trainers found.</p>")

    html_parts.append("<br><p style='color:#999;font-size:12px;'>Generated by Kodely Training Report</p>")
    html_parts.append("</body></html>")
    return "\n".join(html_parts)


def send_email(html: str, subject: str) -> None:
    """Send the training report email via SMTP (same pattern as email_digest.py)."""
    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send training report '%s'", subject)
        return

    to_addrs = [a.strip() for a in config.EMAIL_TO.split(",") if a.strip()]
    cc_addrs = [a.strip() for a in getattr(config, "EMAIL_CC", "").split(",") if a.strip()]
    all_recipients = to_addrs + cc_addrs

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_FROM
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg.attach(MIMEText(html, "html"))

    context = ssl.create_default_context(cafile=certifi.where())
    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
        server.starttls(context=context)
        server.login(config.SMTP_USER, config.SMTP_PASSWORD)
        server.sendmail(config.EMAIL_FROM, all_recipients, msg.as_string())

    log.info("Training report email sent to %s (cc: %s)",
             ", ".join(to_addrs), ", ".join(cc_addrs) or "none")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Training Reminder Report")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report and DMs without posting to Slack",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Send HTML email in addition to Slack",
    )
    parser.add_argument(
        "--email-only",
        action="store_true",
        help="Send HTML email only (no Slack)",
    )
    args = parser.parse_args()

    send_slack = not args.email_only
    send_html = args.email or args.email_only

    # --- Query Notion ---
    leaders = query_leaders_needing_training()

    # --- Group by week ---
    buckets = group_by_week(leaders) if leaders else {
        "overdue": [], "this_week": [], "next_week": [], "later": [],
    }

    # --- Build and post Slack report ---
    if send_slack:
        if not leaders:
            log.info("All leaders have a trainer assigned. Nothing to do for Slack.")
        else:
            report = build_report_message(buckets)
            if args.dry_run:
                print("--- DRY RUN: SLACK #ops-onboarding ---")
                print(report)
                print()
            else:
                slack = SlackClient(token=config.SLACK_BOT_TOKEN)
                try:
                    post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, report)
                    log.info("Posted training reminder report to #ops-onboarding")
                except Exception:
                    log.exception("Failed to post training reminder report")

    # --- Send DM reminders to leaders (only when posting to Slack) ---
    if send_slack and leaders:
        state = load_state()
        slack_dm = SlackClient(token=config.SLACK_BOT_TOKEN) if not args.dry_run else None

        reminded = send_leader_reminders(
            buckets,
            slack=slack_dm,
            state=state,
            dry_run=args.dry_run,
        )

        if not args.dry_run:
            save_state(state)
            log.info("State saved to %s", config.TRAINING_REMINDER_STATE_PATH)
    else:
        reminded = 0

    # --- HTML Email ---
    if send_html:
        log.info("Fetching Calendly data for email report...")
        try:
            user = get_current_user()
            org_uri = user["current_organization"]

            expedited = fetch_expedited_events(org_uri)
            upcoming = fetch_upcoming_training_sessions(org_uri)
        except Exception:
            log.exception("Failed to fetch Calendly events for email")
            expedited, upcoming = [], []

        recently_assigned = fetch_recently_assigned_trainers()

        subject = f"Kodely Training Report — {date.today().strftime('%b %d, %Y')}"
        html = build_email_html(buckets, expedited, upcoming, recently_assigned)

        if args.dry_run:
            print("--- DRY RUN: TRAINING EMAIL ---")
            print(f"Subject: {subject}")
            print(html)
            print()
        else:
            try:
                send_email(html, subject)
            except Exception:
                log.exception("Failed to send training report email")

    log.info(
        "Done. %d leader(s) need training, %d reminded%s.",
        sum(len(v) for v in buckets.values()),
        reminded,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
