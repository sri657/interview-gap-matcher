#!/usr/bin/env python3
"""
Daily Onboarding Status Digest & Compliance Escalation

Queries the Notion onboarding DB for leaders with Readiness Status = "Onboarding",
checks which tasks are complete/incomplete, and posts:
  1. A daily morning digest to #ops-onboarding (--digest-only)
  2. Real-time compliance/urgency alerts (--compliance-only)

Usage:
  python onboarding_digest.py --dry-run          # print everything, post nothing
  python onboarding_digest.py --digest-only       # morning digest only
  python onboarding_digest.py --compliance-only   # compliance checks only
  python onboarding_digest.py                     # both
"""

import argparse
import json
import logging
import os
import ssl
import time
from datetime import date, datetime, timezone

import certifi
import gspread
import httpx
from google.oauth2.service_account import Credentials as ServiceCredentials
import google.auth.transport.requests
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from matcher import _get_worksheet_by_gid

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

# Map Notion property names -> human-readable task names
TASK_FIELDS = {
    config.OB_COMPLIANCE_STATUS_PROPERTY: "Compliance",
    config.OB_GUSTO_PROPERTY: "Gusto",
    config.OB_SLACK_INVITE_PROPERTY: "Slack Invite",
    config.OB_WORKSHOP_SLACK_PROPERTY: "Workshop Slack",
    config.OB_LESSON_PLAN_PROPERTY: "Lesson Plan",
    config.OB_ONBOARDING_EMAIL_PROPERTY: "Onboarding Email",
}


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def load_digest_state() -> dict:
    try:
        with open(config.DIGEST_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_digest_state(data: dict) -> None:
    with open(config.DIGEST_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------

def query_onboarding_leaders() -> list[dict]:
    """Query Notion DB for all leaders with Readiness Status = 'Onboarding'.

    Returns raw Notion page objects with pagination.
    """
    notion_filter = {
        "or": [
            {"property": "Readiness Status", "select": {"equals": "Matched"}},
            {"property": "Readiness Status", "select": {"equals": "Background Check Pending"}},
            {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
            {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
            {"property": "Readiness Status", "select": {"equals": "ACTIVE"}},
            {"property": "Readiness Status", "select": {"equals": "Returning Leader- Onboarding Needed"}},
            {"property": "Readiness Status", "select": {"equals": "Onboarding"}},
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

    log.info("Queried %d onboarding leaders from Notion", len(results))
    return results


def _get_property_value(page: dict, prop_name: str) -> str:
    """Extract the display value from a Notion page property.

    Handles select, status, and date types.
    """
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    prop_type = prop.get("type", "")

    if prop_type == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if prop_type == "status":
        st = prop.get("status")
        return st.get("name", "") if st else ""
    if prop_type == "date":
        dt = prop.get("date")
        return dt.get("start", "") if dt else ""
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_leader_name(page: dict) -> str:
    """Extract leader name from the title property."""
    props = page.get("properties", {})
    # The title property has an empty-string key in this DB
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_region(page: dict) -> str:
    return _get_property_value(page, "Region")


def _get_start_date(page: dict) -> date | None:
    """Parse the Start Date property into a date object."""
    val = _get_property_value(page, "Start Date")
    if not val:
        return None
    try:
        return date.fromisoformat(val[:10])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Task completion checks
# ---------------------------------------------------------------------------

def _is_task_complete(value: str) -> bool:
    """Check if a field value indicates the task is done."""
    return value.strip() in config.OB_DONE_VALUES


def _get_completed_tasks(page: dict) -> list[str]:
    """Return human-readable names of completed tasks."""
    completed = []
    for prop_name, display_name in TASK_FIELDS.items():
        value = _get_property_value(page, prop_name)
        if _is_task_complete(value):
            completed.append(display_name)
    return completed


def _get_incomplete_tasks(page: dict) -> list[str]:
    """Return human-readable names of incomplete tasks."""
    incomplete = []
    for prop_name, display_name in TASK_FIELDS.items():
        value = _get_property_value(page, prop_name)
        if not _is_task_complete(value):
            incomplete.append(display_name)
    return incomplete


# ---------------------------------------------------------------------------
# Pipeline transition helpers
# ---------------------------------------------------------------------------

def _compliance_started(page: dict) -> bool:
    """Return True if Compliance Status is no longer 'Not Sent' or empty."""
    val = _get_property_value(page, config.OB_COMPLIANCE_STATUS_PROPERTY).strip()
    return val not in ("Not Sent", "")


def _all_access_complete(page: dict) -> bool:
    """Return True if ALL onboarding access fields are in done values."""
    return all(
        _is_task_complete(_get_property_value(page, field))
        for field in config.OB_ACCESS_FIELDS
    )


# Transition messages keyed by target stage
_TRANSITION_MESSAGES = {
    "Background Check Pending": "Compliance check has been initiated.",
    "Onboarding Setup": "Background check cleared — ready for access setup.",
    "Onboarding Setup (returning)": "Returning leader — Gusto reactivated, ready for access setup.",
    "Training In Progress": "All onboarding access granted — waiting on training.",
    "ACTIVE": "Training complete — please set up Gusto for this leader.",
}


def _check_transition(page: dict) -> tuple[str, str] | None:
    """Determine if a page should advance to the next pipeline stage.

    Returns (new_stage, message) or None if no transition applies.
    """
    status = _get_property_value(page, "Readiness Status")

    if status == "Matched":
        if _compliance_started(page):
            return "Background Check Pending", _TRANSITION_MESSAGES["Background Check Pending"]

    elif status == "Background Check Pending":
        compliance_val = _get_property_value(page, config.OB_COMPLIANCE_STATUS_PROPERTY)
        if _is_task_complete(compliance_val):
            return "Onboarding Setup", _TRANSITION_MESSAGES["Onboarding Setup"]

    elif status == "Onboarding Setup":
        if _all_access_complete(page):
            return "Training In Progress", _TRANSITION_MESSAGES["Training In Progress"]

    elif status == "Returning Leader- Onboarding Needed":
        gusto_val = _get_property_value(page, config.OB_GUSTO_PROPERTY)
        if _is_task_complete(gusto_val):
            return "Onboarding Setup", _TRANSITION_MESSAGES["Onboarding Setup (returning)"]

    elif status == "Training In Progress":
        training_val = _get_property_value(page, config.OB_TRAINING_STATUS_PROPERTY)
        if _is_task_complete(training_val):
            return "ACTIVE", _TRANSITION_MESSAGES["ACTIVE"]

    return None


def _patch_readiness_status(page_id: str, new_stage: str) -> bool:
    """Update a Notion page's Readiness Status. Returns True on success."""
    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {"Readiness Status": {"select": {"name": new_stage}}}},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Failed to advance page %s to %s: %s %s", page_id, new_stage, resp.status_code, resp.text)
        return False
    return True


# Map target stage -> Ops Hub cell color (only stages that change color)
_STAGE_COLOR_MAP = {
    "Background Check Pending": config.CELL_COLOR_PURPLE,
    "Onboarding Setup": config.CELL_COLOR_GREEN,
}


def _find_leader_cells(sheet_data: list[list[str]], leader_name: str) -> list[tuple[int, int]]:
    """Search columns S(18), T(19), U(20) for exact name match.

    Returns list of (row_index, col_index) tuples (0-based).
    """
    matches = []
    for row_idx, row in enumerate(sheet_data):
        for col_idx in (18, 19, 20):  # S, T, U columns (Leader 1, Leader 2, Leader 3)
            if col_idx < len(row) and row[col_idx].strip() == leader_name:
                matches.append((row_idx, col_idx))
    return matches


def _update_cell_color(
    creds: ServiceCredentials, row: int, col: int, color_rgb: dict
) -> None:
    """Set a single cell's background color in the Ops Hub sheet via Sheets API."""
    creds_copy = creds.with_scopes(["https://www.googleapis.com/auth/spreadsheets"])
    creds_copy.refresh(google.auth.transport.requests.Request())

    # row/col are 0-based into the data (including header row 0)
    request_body = {
        "requests": [
            {
                "updateCells": {
                    "rows": [
                        {
                            "values": [
                                {
                                    "userEnteredFormat": {
                                        "backgroundColor": color_rgb,
                                    }
                                }
                            ]
                        }
                    ],
                    "fields": "userEnteredFormat.backgroundColor",
                    "range": {
                        "sheetId": config.SHEET_GID,
                        "startRowIndex": row,
                        "endRowIndex": row + 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                }
            }
        ]
    }

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{config.GOOGLE_SHEET_ID}:batchUpdate"
    headers = {"Authorization": f"Bearer {creds_copy.token}"}
    resp = httpx.post(url, headers=headers, json=request_body, timeout=30)
    if resp.status_code >= 400:
        log.error("Failed to update cell color at row=%d col=%d: %s %s", row, col, resp.status_code, resp.text)
    else:
        log.info("Updated cell color at row=%d col=%d", row, col)


def advance_pipeline(
    leaders: list[dict],
    state: dict,
    slack: "SlackClient",
    dry_run: bool = False,
    sheet_data: list[list[str]] | None = None,
    creds: ServiceCredentials | None = None,
) -> list[str]:
    """Check each leader for pipeline transitions and advance if conditions are met.

    Returns list of transition messages. Updates state dict to track notified transitions.
    """
    messages = []

    for page in leaders:
        page_id = page.get("id", "")
        name = _get_leader_name(page)
        region = _get_region(page)

        if not name:
            continue

        result = _check_transition(page)
        if result is None:
            continue

        new_stage, reason = result
        current_stage = _get_property_value(page, "Readiness Status")

        # Deduplicate: don't re-advance if we already moved this page in a previous run
        pipeline_key = f"pipeline_{page_id}_{new_stage}"
        if state.get(pipeline_key):
            continue

        if dry_run:
            print(f"--- DRY RUN: PIPELINE ADVANCE ---")
            print(f"  {name} ({region}): {current_stage} → {new_stage}")
            print(f"  Reason: {reason}")
            color = _STAGE_COLOR_MAP.get(new_stage)
            if color and sheet_data:
                cells = _find_leader_cells(sheet_data, name)
                print(f"  Color sync: would update {len(cells)} cell(s) to {new_stage} color")
            print()
        else:
            if not _patch_readiness_status(page_id, new_stage):
                continue

            # Sync Ops Hub cell color if this transition has a color mapping
            color = _STAGE_COLOR_MAP.get(new_stage)
            if color and sheet_data and creds:
                cells = _find_leader_cells(sheet_data, name)
                for r, c in cells:
                    try:
                        _update_cell_color(creds, r, c, color)
                    except Exception:
                        log.exception("Failed to update cell color for %s at (%d,%d)", name, r, c)

            msg = (
                f"\U0001f4ca PIPELINE UPDATE\n\n"
                f"Leader: {name} \u2192 {new_stage}\n"
                f"{reason}"
            )
            try:
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
                log.info("Pipeline: %s moved to %s", name, new_stage)
            except Exception:
                log.exception("Failed to post pipeline update for %s", name)

            messages.append(f"{name}: {current_stage} → {new_stage}")

        state[pipeline_key] = True

    return messages


# ---------------------------------------------------------------------------
# Slack helpers (same pattern as onboarding_tracker.py)
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
# Automation 1: Daily Digest
# ---------------------------------------------------------------------------

def build_digest_message(leaders: list[dict]) -> str:
    """Build the daily onboarding status digest Slack message.

    Leaders are grouped into:
      - URGENT: start date < OB_URGENT_DAYS days away, with incomplete tasks
      - WARNING: start date < OB_WARNING_DAYS days away, with incomplete tasks
      - IN PROGRESS: further out, with incomplete tasks
      - Fully onboarded: all tasks complete (count only)
    """
    today = date.today()
    urgent = []
    warning = []
    in_progress = []
    fully_done = 0

    for page in leaders:
        name = _get_leader_name(page)
        region = _get_region(page)
        start = _get_start_date(page)
        completed = _get_completed_tasks(page)
        incomplete = _get_incomplete_tasks(page)

        if not incomplete:
            fully_done += 1
            continue

        days_until = (start - today).days if start else 999

        entry = {
            "name": name,
            "region": region,
            "start": start,
            "days_until": days_until,
            "completed": completed,
            "incomplete": incomplete,
        }

        if days_until < config.OB_URGENT_DAYS:
            urgent.append(entry)
        elif days_until < config.OB_WARNING_DAYS:
            warning.append(entry)
        else:
            in_progress.append(entry)

    # Sort each group by start date (soonest first)
    for group in (urgent, warning, in_progress):
        group.sort(key=lambda e: e["start"] or date.max)

    date_str = today.strftime("%b %d, %Y")
    total = len(leaders)
    lines = [
        f"\U0001f4cb DAILY ONBOARDING STATUS DIGEST",
        f"{date_str} \u2014 {total} leader{'s' if total != 1 else ''} actively onboarding",
    ]

    def _format_entry(entry: dict) -> str:
        start_str = ""
        if entry["start"]:
            d = entry["days_until"]
            if d < 0:
                start_str = f" \u2014 Started {entry['start'].strftime('%b %d')} ({-d} day{'s' if -d != 1 else ''} ago)"
            else:
                start_str = f" \u2014 Starts {entry['start'].strftime('%b %d')} ({d} day{'s' if d != 1 else ''})"
        done_str = ", ".join(entry["completed"]) if entry["completed"] else "None"
        todo_str = ", ".join(entry["incomplete"]) if entry["incomplete"] else "None"
        return (
            f"> {entry['name']} \u2014 {entry['region']}{start_str}\n"
            f"> \u2705 {done_str}\n"
            f"> \u274c {todo_str}"
        )

    if urgent:
        lines.append("")
        lines.append(f"\U0001f6a8 URGENT \u2014 Starting in <{config.OB_URGENT_DAYS} days with incomplete tasks:")
        lines.append("")
        for entry in urgent:
            lines.append(_format_entry(entry))
            lines.append("")

    if warning:
        lines.append(f"\u26a0\ufe0f WARNING \u2014 Starting in <{config.OB_WARNING_DAYS} days:")
        lines.append("")
        for entry in warning:
            lines.append(_format_entry(entry))
            lines.append("")

    if in_progress:
        lines.append(f"\u23f3 IN PROGRESS:")
        lines.append("")
        for entry in in_progress:
            lines.append(_format_entry(entry))
            lines.append("")

    if fully_done:
        lines.append(f"{fully_done} leader{'s' if fully_done != 1 else ''} fully onboarded (not shown)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Automation 2: Compliance Tracking & Escalation
# ---------------------------------------------------------------------------

def check_compliance_alerts(leaders: list[dict], state: dict, dry_run: bool = False) -> list[str]:
    """Check each leader for compliance status changes and urgency.

    Returns list of alert messages to post. Updates state dict in place.
    """
    today = date.today()
    alerts = []

    for page in leaders:
        page_id = page.get("id", "")
        name = _get_leader_name(page)
        region = _get_region(page)
        start = _get_start_date(page)
        compliance_val = _get_property_value(page, config.OB_COMPLIANCE_STATUS_PROPERTY)
        incomplete = _get_incomplete_tasks(page)

        if not name:
            continue

        leader_state = state.get(page_id, {})
        last_status = leader_state.get("last_status", "")
        days_until = (start - today).days if start else 999
        start_str = start.strftime("%b %d, %Y") if start else "TBD"

        # --- Compliance Approved (celebration) ---
        if _is_task_complete(compliance_val) and not leader_state.get("approved_notified"):
            msg = (
                f"\U0001f389 COMPLIANCE APPROVED\n\n"
                f"*Leader:* {name}\n"
                f"*Region:* {region}\n"
                f"*Starts:* {start_str}\n\n"
                f"Background check is clear \u2014 ready for remaining onboarding steps."
            )
            alerts.append(msg)
            leader_state["approved_notified"] = True

        # --- Urgent: starting soon (or already started), compliance not approved ---
        if (
            days_until < config.OB_URGENT_DAYS
            and not _is_task_complete(compliance_val)
            and not leader_state.get("urgent_notified")
        ):
            if days_until < 0:
                timing_header = f"STARTED {-days_until} DAY{'S' if -days_until != 1 else ''} AGO"
                timing_body = f"This leader already started {-days_until} day{'s' if -days_until != 1 else ''} ago but compliance is not yet approved."
            else:
                timing_header = f"STARTING IN {days_until} DAY{'S' if days_until != 1 else ''}"
                timing_body = f"This leader starts in {days_until} day{'s' if days_until != 1 else ''} but compliance is not yet approved."
            msg = (
                f"\U0001f6a8 URGENT: COMPLIANCE NOT APPROVED \u2014 {timing_header}\n\n"
                f"*Leader:* {name}\n"
                f"*Region:* {region}\n"
                f"*Start Date:* {start_str}\n"
                f"*Compliance Status:* {compliance_val or 'Not Set'}\n\n"
                f"{timing_body}\n"
                f"Immediate action required."
            )
            alerts.append(msg)
            leader_state["urgent_notified"] = True

        # --- Warning: starting within 7 days, tasks incomplete ---
        if (
            config.OB_URGENT_DAYS <= days_until < config.OB_WARNING_DAYS
            and incomplete
            and not leader_state.get("warning_notified")
        ):
            incomplete_lines = "\n".join(f"\u274c {t}" for t in incomplete)
            msg = (
                f"\u26a0\ufe0f WARNING: INCOMPLETE ONBOARDING \u2014 STARTING IN {days_until} DAY{'S' if days_until != 1 else ''}\n\n"
                f"*Leader:* {name}\n"
                f"*Region:* {region}\n"
                f"*Start Date:* {start_str}\n\n"
                f"Incomplete items:\n"
                f"{incomplete_lines}\n\n"
                f"Please prioritize completing these before the start date."
            )
            alerts.append(msg)
            leader_state["warning_notified"] = True

        # Track status changes
        leader_state["last_status"] = compliance_val
        state[page_id] = leader_state

    return alerts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Onboarding Digest & Compliance Tracker")
    parser.add_argument("--dry-run", action="store_true", help="Print, don't post")
    parser.add_argument("--digest-only", action="store_true", help="Only run the morning digest")
    parser.add_argument("--compliance-only", action="store_true", help="Only run compliance checks")
    args = parser.parse_args()

    run_digest = not args.compliance_only
    run_compliance = not args.digest_only

    slack = SlackClient(token=config.SLACK_BOT_TOKEN)
    leaders = query_onboarding_leaders()

    if not leaders:
        log.info("No onboarding leaders found. Exiting.")
        return

    # --- Daily Digest ---
    if run_digest:
        log.info("Building daily digest for %d leaders...", len(leaders))
        digest_msg = build_digest_message(leaders)

        if args.dry_run:
            print("--- DRY RUN: DAILY DIGEST (#ops-onboarding) ---")
            print(digest_msg)
            print()
        else:
            try:
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, digest_msg)
                log.info("Posted daily digest to #ops-onboarding")
            except Exception:
                log.exception("Failed to post daily digest")

    # --- Compliance Checks ---
    if run_compliance:
        log.info("Running compliance checks for %d leaders...", len(leaders))
        digest_state = load_digest_state()
        alerts = check_compliance_alerts(leaders, digest_state, dry_run=args.dry_run)

        if alerts:
            log.info("Generated %d compliance alert(s)", len(alerts))
            for alert in alerts:
                if args.dry_run:
                    print("--- DRY RUN: COMPLIANCE ALERT (#ops-onboarding) ---")
                    print(alert)
                    print()
                else:
                    try:
                        post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, alert)
                    except Exception:
                        log.exception("Failed to post compliance alert")
        else:
            log.info("No new compliance alerts.")

        # --- Init Google Sheets for Ops Hub color sync ---
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        gs_creds = ServiceCredentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_PATH, scopes=scopes
        )
        gc = gspread.authorize(gs_creds)
        spreadsheet = gc.open_by_key(config.GOOGLE_SHEET_ID)
        sheet = _get_worksheet_by_gid(spreadsheet, config.SHEET_GID)
        sheet_data = sheet.get_all_values()
        log.info("Loaded %d rows from Ops Hub for color sync", len(sheet_data))

        # --- Pipeline Auto-Advance ---
        log.info("Checking pipeline transitions for %d leaders...", len(leaders))
        transitions = advance_pipeline(
            leaders, digest_state, slack,
            dry_run=args.dry_run,
            sheet_data=sheet_data,
            creds=gs_creds,
        )
        if transitions:
            log.info("Advanced %d leader(s) in pipeline: %s", len(transitions), "; ".join(transitions))
        else:
            log.info("No pipeline transitions.")

        if not args.dry_run:
            save_digest_state(digest_state)
            log.info("Digest state saved to %s", config.DIGEST_STATE_PATH)

    log.info("Done%s.", " (dry run)" if args.dry_run else "")


if __name__ == "__main__":
    main()
