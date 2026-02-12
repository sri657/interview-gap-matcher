#!/usr/bin/env python3
"""
Auto-Onboarding & Offboarding Tracker

Reads the Ops Hub sheet, detects new green (onboarding), purple (compliance),
and red/strikethrough (backout) cells, creates Notion onboarding tickets,
and posts Slack alerts to #ops-onboarding / #ops-offboarding.
"""

import argparse
import json
import logging
import os
import re
import ssl
import time
from datetime import datetime, timezone, date

import certifi
import gspread
import httpx
from google.oauth2.service_account import Credentials as ServiceCredentials
import google.auth.transport.requests
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from matcher import (
    _classify_leader_cell,
    _fetch_leader_formatting,
    _get_worksheet_by_gid,
    _parse_date,
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

NOTION_BASE = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {config.NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# Cell classes that trigger onboarding/offboarding events
ONBOARDING_CLASSES = {"orange"}
COMPLIANCE_CLASSES = {"purple"}
OFFBOARDING_CLASSES = {"red", "strikethrough"}

# All classes we track in state
TRACKED_CLASSES = ONBOARDING_CLASSES | COMPLIANCE_CLASSES | OFFBOARDING_CLASSES


# ---------------------------------------------------------------------------
# State file helpers (same pattern as notified.json)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.ONBOARDED_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.ONBOARDED_PATH, "w") as f:
        json.dump(data, f, indent=2)


def state_key(leader_name: str, workshop_key: str, cell_class: str) -> str:
    return f"{leader_name}::{workshop_key}::{cell_class}"


# ---------------------------------------------------------------------------
# Slack user-ID lookup (for @mentions)
# ---------------------------------------------------------------------------

def _lookup_slack_user_ids(slack: SlackClient, emails: list[str]) -> dict[str, str]:
    """Map email -> Slack user ID. Returns empty dict on failure."""
    mapping = {}
    for email in emails:
        try:
            resp = slack.users_lookupByEmail(email=email)
            if resp["ok"]:
                mapping[email] = resp["user"]["id"]
        except SlackApiError:
            log.debug("Could not look up Slack user for %s (users:read scope needed?)", email)
    return mapping


# ---------------------------------------------------------------------------
# Read Ops Hub and classify every leader cell
# ---------------------------------------------------------------------------

def scan_leader_cells(
    gc: gspread.Client, creds: ServiceCredentials
) -> list[dict]:
    """Return a list of events: one per leader cell that has a tracked status.

    Each event dict has:
      leader_name, workshop_key, cell_class, region, site, lesson, day, time,
      start_date, end_date, district
    """
    spreadsheet = gc.open_by_key(config.GOOGLE_SHEET_ID)
    sheet = _get_worksheet_by_gid(spreadsheet, config.SHEET_GID)

    all_rows = sheet.get_all_values()
    if not all_rows:
        return []

    raw_headers = all_rows[0]
    valid_cols = [i for i, h in enumerate(raw_headers) if h.strip()]
    headers = [raw_headers[i] for i in valid_cols]
    col_map = {h: j for j, h in enumerate(headers)}

    records = []
    for row in all_rows[1:]:
        records.append(
            {headers[j]: (row[i] if i < len(row) else "") for j, i in enumerate(valid_cols)}
        )

    log.info("Fetching leader column formatting for %d rows...", len(records))
    fmt_map = _fetch_leader_formatting(creds, len(records))

    today = date.today()
    events = []

    leader_cols = [config.SHEET_LEADER_1_COL, config.SHEET_LEADER_2_COL, config.SHEET_LEADER_3_COL]

    for idx, row in enumerate(records):
        # Skip cancelled programs
        setup = row.get("Setup", "").strip().upper()
        if "CANCEL" in setup:
            continue

        # Skip ended workshops
        end_date_str = row.get("End Date", "")
        end_dt = _parse_date(end_date_str)
        if end_dt and end_dt < today:
            continue

        region = row.get(config.SHEET_REGION_COL, "").strip()
        site = row.get(config.SHEET_SITE_COL, "").strip()
        if not region and not site:
            continue

        lesson = row.get(config.SHEET_LESSON_COL, "").strip()
        day = row.get(config.SHEET_DAY_COL, "").strip()
        start_time = row.get(config.SHEET_START_TIME_COL, "").strip()
        end_time = row.get(config.SHEET_END_TIME_COL, "").strip()
        time_str = f"{start_time}-{end_time}" if start_time and end_time else start_time or end_time
        start_date_str = row.get("Start Date", "").strip()
        district = row.get(config.SHEET_DISTRICT_COL, "").strip()

        workshop_key = f"{region}|{site}|{lesson}|{day}|{time_str}"

        cell_classes = fmt_map.get(idx, ["normal", "normal", "normal"])

        for col_name, cls in zip(leader_cols, cell_classes):
            if cls not in TRACKED_CLASSES:
                continue
            raw_value = row.get(col_name, "").strip()
            if not raw_value:
                continue

            # Extract email if embedded in the cell (e.g. "Name email@x.com" or "Name\nemail@x.com")
            leader_name = raw_value
            leader_email = ""
            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', raw_value)
            if email_match:
                leader_email = email_match.group(0)
                leader_name = raw_value.replace(leader_email, "").replace("\n", " ").strip()
            # Strip parenthetical notes like "(TUES,FRI)" or "(WED-FRI)"
            leader_name = re.sub(r'\s*\([^)]*\)\s*$', '', leader_name).strip()

            events.append({
                "leader_name": leader_name,
                "leader_email": leader_email,
                "workshop_key": workshop_key,
                "cell_class": cls,
                "region": region,
                "site": site,
                "lesson": lesson or "(unnamed)",
                "day": day,
                "time": time_str,
                "start_date": start_date_str,
                "end_date": end_date_str,
                "district": district,
            })

    log.info(
        "Detected %d tracked leader cell(s): %d onboarding, %d compliance, %d offboarding",
        len(events),
        sum(1 for e in events if e["cell_class"] in ONBOARDING_CLASSES),
        sum(1 for e in events if e["cell_class"] in COMPLIANCE_CLASSES),
        sum(1 for e in events if e["cell_class"] in OFFBOARDING_CLASSES),
    )
    return events


# ---------------------------------------------------------------------------
# Notion: create onboarding page
# ---------------------------------------------------------------------------

def _find_existing_onboarding_page(leader_name: str) -> tuple[str, str] | None:
    """Check if a page with this leader name already exists in the onboarding DB.

    Returns (page_id, page_url) or None.
    """
    body = {
        "filter": {
            "property": "",
            "title": {"equals": leader_name},
        },
        "page_size": 1,
    }
    resp = httpx.post(
        f"{NOTION_BASE}/databases/{config.ONBOARDING_DB_ID}/query",
        headers=NOTION_HEADERS,
        json=body,
        timeout=30,
    )
    if resp.status_code >= 400:
        return None
    data = resp.json()
    results = data.get("results", [])
    if results:
        return results[0]["id"], results[0].get("url", "")
    return None


def _update_returning_leader_page(
    page_id: str,
    region: str,
    site: str,
    start_date_str: str,
) -> str | None:
    """Update an existing Notion page for a returning leader.

    Clears trainer assignment, sets status to Returning Leader- Onboarding Needed,
    and updates school/region/start date for the new assignment.
    """
    start_date_iso = None
    parsed = _parse_date(start_date_str)
    if parsed:
        start_date_iso = parsed.isoformat()

    properties: dict = {
        "Readiness Status": {"select": {"name": "Returning Leader- Onboarding Needed"}},
        "Trainer Assigned": {"select": None},
        "School Teaching": {"multi_select": [{"name": site}]},
    }
    if region:
        properties["Region"] = {"select": {"name": region}}
    if start_date_iso:
        properties["Start Date"] = {"date": {"start": start_date_iso}}

    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Notion API error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    page = resp.json()
    return page.get("url")


def create_onboarding_page(
    leader_name: str,
    region: str,
    site: str,
    start_date_str: str,
) -> tuple[str | None, bool]:
    """Create or update a page in the Notion onboarding DB.

    Returns (page_url, is_returning). If a page already exists the leader is
    treated as returning: their card is updated instead of creating a new one.
    """
    existing = _find_existing_onboarding_page(leader_name)
    if existing:
        page_id, page_url = existing
        log.info("Returning leader %s — updating existing Notion page", leader_name)
        url = _update_returning_leader_page(page_id, region, site, start_date_str)
        return url or page_url, True

    start_date_iso = None
    parsed = _parse_date(start_date_str)
    if parsed:
        start_date_iso = parsed.isoformat()

    properties: dict = {
        "": {"title": [{"text": {"content": leader_name}}]},
        "Season": {"select": {"name": "Winter 2026"}},
        "Readiness Status": {"select": {"name": "Matched"}},
        "School Teaching": {"multi_select": [{"name": site}]},
        "Compliance Status": {"select": {"name": "Not Sent"}},
        "Leader Type": {"select": {"name": "Leader"}},
    }
    if region:
        properties["Region"] = {"select": {"name": region}}
    if start_date_iso:
        properties["Start Date"] = {"date": {"start": start_date_iso}}

    body = {
        "parent": {"database_id": config.ONBOARDING_DB_ID},
        "properties": properties,
    }

    resp = httpx.post(
        f"{NOTION_BASE}/pages",
        headers=NOTION_HEADERS,
        json=body,
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Notion API error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    page = resp.json()
    return page.get("url"), False


# ---------------------------------------------------------------------------
# Notion: create offboarding page
# ---------------------------------------------------------------------------

def create_offboarding_page(
    leader_name: str,
    region: str,
    site: str,
    start_date_str: str,
) -> str | None:
    """Create an offboarding page in the Notion onboarding DB with a checklist.

    Returns the page URL or None. Skips if a page already exists.
    """
    existing = _find_existing_onboarding_page(leader_name)
    if existing:
        _page_id, page_url = existing
        log.info("Notion page already exists for %s, skipping offboarding creation", leader_name)
        return page_url

    start_date_iso = None
    parsed = _parse_date(start_date_str)
    if parsed:
        start_date_iso = parsed.isoformat()

    properties: dict = {
        "": {"title": [{"text": {"content": leader_name}}]},
        "Season": {"select": {"name": "Winter 2026"}},
        "Readiness Status": {"select": {"name": "Offboarding Needed"}},
        "School Teaching": {"multi_select": [{"name": site}]},
        "Leader Type": {"select": {"name": "Leader"}},
    }
    if region:
        properties["Region"] = {"select": {"name": region}}
    if start_date_iso:
        properties["Start Date"] = {"date": {"start": start_date_iso}}

    # Offboarding checklist as Notion to_do blocks
    checklist_items = [
        "Remove from Gusto",
        "Remove from Slack workspace",
        "Remove LearnDash access",
        "Remove from Workshop Slack channel",
        "Notify team of departure",
    ]
    children = [
        {
            "object": "block",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "text": {"content": item}}],
                "checked": False,
            },
        }
        for item in checklist_items
    ]

    body = {
        "parent": {"database_id": config.ONBOARDING_DB_ID},
        "properties": properties,
        "children": children,
    }

    resp = httpx.post(
        f"{NOTION_BASE}/pages",
        headers=NOTION_HEADERS,
        json=body,
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Notion API error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    page = resp.json()
    return page.get("url")


# ---------------------------------------------------------------------------
# Slack alert builders
# ---------------------------------------------------------------------------

def _format_dates(start: str, end: str) -> str:
    """Format date range for display, e.g. 'Jan 15, 2026 - Mar 20, 2026'."""
    parts = []
    parsed_start = _parse_date(start)
    parsed_end = _parse_date(end)
    if parsed_start:
        parts.append(parsed_start.strftime("%b %d, %Y"))
    if parsed_end:
        parts.append(parsed_end.strftime("%b %d, %Y"))
    return " \u2013 ".join(parts) if parts else ""


def build_onboarding_alert(
    event: dict,
    notion_url: str | None,
    is_returning: bool = False,
) -> str:
    dates = _format_dates(event["start_date"], event["end_date"])
    dates_line = f"\n*Dates:* {dates}" if dates else ""
    email = event.get("leader_email", "")
    email_line = f"\n*Email:* {email}" if email else ""

    if is_returning:
        notion_line = ""
        if notion_url:
            notion_line = f"\n:arrows_counterclockwise: Notion card updated \u2192 {notion_url}"
        return (
            f":arrows_counterclockwise: *RETURNING LEADER \u2014 NEW SCHOOL ASSIGNMENT*\n\n"
            f"*Leader:* {event['leader_name']}{email_line}\n"
            f"*New School:* {event['site']} ({event['region']})\n"
            f"*Program:* {event['lesson']} \u2014 {event['day']}s {event['time']}"
            f"{dates_line}"
            f"{notion_line}\n\n"
            f"This leader has been assigned to a new school. Please add them to it and onboard.\n"
            f":clipboard: *Next steps:* Verify if Checkr is cleared, add to new school & onboard"
        )

    notion_line = ""
    if notion_url:
        notion_line = f"\n:white_check_mark: Notion ticket created \u2192 {notion_url}"

    return (
        f":large_green_circle: *NEW ONBOARDING NEEDED*\n\n"
        f"*Leader:* {event['leader_name']}{email_line}\n"
        f"*School:* {event['site']} ({event['region']})\n"
        f"*Program:* {event['lesson']} \u2014 {event['day']}s {event['time']}"
        f"{dates_line}"
        f"{notion_line}\n\n"
        f":clipboard: *Next step:* Kick off Checkr background check"
    )


def build_offboarding_alert(event: dict, notion_url: str | None = None) -> str:
    dates = _format_dates(event["start_date"], event["end_date"])
    dates_line = f"\n*Dates:* {dates}" if dates else ""

    notion_line = ""
    if notion_url:
        notion_line = f"\n:white_check_mark: Notion offboarding ticket created \u2192 {notion_url}"

    return (
        f":red_circle: *LEADER BACKED OUT \u2014 RESTAFFING NEEDED*\n\n"
        f"*Leader:* {event['leader_name']}\n"
        f"*School:* {event['site']} ({event['region']})\n"
        f"*Program:* {event['lesson']} \u2014 {event['day']}s {event['time']}"
        f"{dates_line}"
        f"{notion_line}\n\n"
        f":warning: This is now a gap \u2014 check the gap match digest for replacement candidates."
    )


def build_compliance_alert(event: dict) -> str:
    dates = _format_dates(event["start_date"], event["end_date"])
    dates_line = f"\n*Dates:* {dates}" if dates else ""

    return (
        f":purple_circle: *COMPLIANCE STARTED*\n\n"
        f"*Leader:* {event['leader_name']}\n"
        f"*School:* {event['site']} ({event['region']})\n"
        f"*Program:* {event['lesson']} \u2014 {event['day']}s {event['time']}"
        f"{dates_line}"
    )


# ---------------------------------------------------------------------------
# Post to Slack with retries
# ---------------------------------------------------------------------------

def post_to_slack(slack: SlackClient, channel: str, message: str, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            slack.chat_postMessage(channel=channel, text=message)
            return
        except SlackApiError as e:
            if e.response["error"] == "ratelimited" and attempt < retries - 1:
                wait = int(e.response.headers.get("Retry-After", 5))
                log.warning("Rate limited by Slack, waiting %ds...", wait)
                time.sleep(wait)
            else:
                log.error("Slack API error: %s", e.response["error"])
                raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-Onboarding & Offboarding Tracker")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect events and print alerts without posting to Slack or creating Notion pages",
    )
    args = parser.parse_args()

    # --- Initialise clients ---
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceCredentials.from_service_account_file(
        config.GOOGLE_CREDENTIALS_PATH, scopes=scopes
    )
    gc = gspread.authorize(creds)
    slack = SlackClient(token=config.SLACK_BOT_TOKEN)

    # --- Scan Ops Hub for tracked leader cells ---
    events = scan_leader_cells(gc, creds)

    # --- Dedup against state file ---
    state = load_state()
    new_onboarding = []
    new_offboarding = []
    new_compliance = []

    for event in events:
        key = state_key(event["leader_name"], event["workshop_key"], event["cell_class"])
        if key in state:
            continue
        if event["cell_class"] in ONBOARDING_CLASSES:
            new_onboarding.append(event)
        elif event["cell_class"] in OFFBOARDING_CLASSES:
            new_offboarding.append(event)
        elif event["cell_class"] in COMPLIANCE_CLASSES:
            new_compliance.append(event)

    total_new = len(new_onboarding) + len(new_offboarding) + len(new_compliance)
    if total_new == 0:
        log.info("No new onboarding/offboarding events. Exiting.")
        return

    log.info(
        "New events: %d onboarding, %d offboarding, %d compliance",
        len(new_onboarding), len(new_offboarding), len(new_compliance),
    )

    now = datetime.now(timezone.utc).isoformat()

    # --- Process onboarding events ---
    for event in new_onboarding:
        notion_url = None
        is_returning = False
        slack_ok = False
        if args.dry_run:
            existing = _find_existing_onboarding_page(event["leader_name"])
            if existing:
                is_returning = True
            action = "Update returning" if is_returning else "Create"
            print(f"--- DRY RUN: NOTION PAGE ({action}) ---")
            print(f"  Would {action.lower()} onboarding page for: {event['leader_name']}")
            print(f"  Region: {event['region']}, Site: {event['site']}")
            print(f"  Start Date: {event['start_date']}")
            print()
        else:
            try:
                notion_url, is_returning = create_onboarding_page(
                    leader_name=event["leader_name"],
                    region=event["region"],
                    site=event["site"],
                    start_date_str=event["start_date"],
                )
                action = "Updated returning" if is_returning else "Created"
                log.info("%s Notion page for %s: %s", action, event["leader_name"], notion_url)
            except Exception:
                log.exception("Failed to create Notion page for %s", event["leader_name"])

        message = build_onboarding_alert(event, notion_url, is_returning)
        if args.dry_run:
            print("--- DRY RUN: SLACK #ops-onboarding ---")
            print(message)
            print()
            slack_ok = True
        else:
            try:
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, message)
                log.info("Posted onboarding alert for %s", event["leader_name"])
                slack_ok = True
            except Exception:
                log.exception("Failed to post onboarding alert for %s", event["leader_name"])

        if slack_ok:
            key = state_key(event["leader_name"], event["workshop_key"], event["cell_class"])
            state[key] = now

    # --- Process offboarding events ---
    for event in new_offboarding:
        notion_url = None
        slack_ok = False
        if args.dry_run:
            print("--- DRY RUN: NOTION OFFBOARDING PAGE ---")
            print(f"  Would create offboarding page for: {event['leader_name']}")
            print(f"  Region: {event['region']}, Site: {event['site']}")
            print(f"  Start Date: {event['start_date']}")
            print()
        else:
            try:
                notion_url = create_offboarding_page(
                    leader_name=event["leader_name"],
                    region=event["region"],
                    site=event["site"],
                    start_date_str=event["start_date"],
                )
                log.info("Created Notion offboarding page for %s: %s", event["leader_name"], notion_url)
            except Exception:
                log.exception("Failed to create Notion offboarding page for %s", event["leader_name"])

        message = build_offboarding_alert(event, notion_url)
        if args.dry_run:
            print("--- DRY RUN: SLACK #ops-offboarding ---")
            print(message)
            print()
            slack_ok = True
        else:
            try:
                post_to_slack(slack, config.SLACK_OFFBOARDING_CHANNEL, message)
                log.info("Posted offboarding alert for %s", event["leader_name"])
                slack_ok = True
            except Exception:
                log.exception("Failed to post offboarding alert for %s", event["leader_name"])

        if slack_ok:
            key = state_key(event["leader_name"], event["workshop_key"], event["cell_class"])
            state[key] = now

    # --- Process compliance events (informational, post to onboarding channel) ---
    for event in new_compliance:
        message = build_compliance_alert(event)
        slack_ok = False
        if args.dry_run:
            print("--- DRY RUN: SLACK #ops-onboarding (compliance) ---")
            print(message)
            print()
            slack_ok = True
        else:
            try:
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, message)
                log.info("Posted compliance alert for %s", event["leader_name"])
                slack_ok = True
            except Exception:
                log.exception("Failed to post compliance alert for %s", event["leader_name"])

        if slack_ok:
            key = state_key(event["leader_name"], event["workshop_key"], event["cell_class"])
            state[key] = now

    # --- Save state (skip in dry-run) ---
    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.ONBOARDED_PATH)
    else:
        log.info("Dry run — state file not updated.")

    log.info(
        "Done. %d onboarding, %d offboarding, %d compliance event(s) processed%s.",
        len(new_onboarding),
        len(new_offboarding),
        len(new_compliance),
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
