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
import smtplib
import ssl
import time
from datetime import date, datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import certifi
import gspread
import httpx
from google.oauth2.service_account import Credentials as ServiceCredentials
import google.auth.transport.requests
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from matcher import _get_worksheet_by_gid

# Lazy imports for pipeline automation hooks (avoid circular imports at module level)
_checkr_sync = None
_welcome_email = None
_slack_provision = None
_trainer_notes = None


def _get_checkr_sync():
    global _checkr_sync
    if _checkr_sync is None:
        import checkr_sync as _cs
        _checkr_sync = _cs
    return _checkr_sync


def _get_welcome_email():
    global _welcome_email
    if _welcome_email is None:
        import welcome_email as _we
        _welcome_email = _we
    return _welcome_email


def _get_slack_provision():
    global _slack_provision
    if _slack_provision is None:
        import slack_provision as _sp
        _slack_provision = _sp
    return _slack_provision


def _get_trainer_notes():
    global _trainer_notes
    if _trainer_notes is None:
        import trainer_notes as _tn
        _trainer_notes = _tn
    return _trainer_notes

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

_STAGE_PRIORITY = {
    "Matched": 0,
    "Background Check Pending": 1,
    "Onboarding Setup": 2,
    "Onboarding": 2,
    "Training In Progress": 3,
    "ACTIVE": 4,
    "Needs Review": 5,
}


def _dedup_leaders(leaders: list[dict]) -> list[dict]:
    """Deduplicate leaders by name, keeping the card with the lowest pipeline stage.

    When a leader has multiple Notion cards (e.g. one Matched and one ACTIVE),
    we keep the one that's most relevant for onboarding.
    """
    by_name: dict[str, tuple[dict, int]] = {}
    for page in leaders:
        name = _get_leader_name(page).strip().lower()
        if not name:
            continue
        status = _get_property_value(page, "Readiness Status")
        priority = _STAGE_PRIORITY.get(status, 5)
        if name not in by_name or priority < by_name[name][1]:
            by_name[name] = (page, priority)
    return [page for page, _ in by_name.values()]


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
            {"property": "Readiness Status", "select": {"equals": "Onboarding"}},
            {"property": "Readiness Status", "select": {"equals": "Needs Review"}},
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


def _get_leader_email(page: dict) -> str:
    """Extract leader email from the Email property."""
    props = page.get("properties", {})
    email_prop = props.get("Email", {})
    if email_prop.get("type") == "email":
        return (email_prop.get("email") or "").strip().lower()
    if email_prop.get("type") == "rich_text":
        parts = email_prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in parts).strip().lower()
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
    "Training In Progress": "All onboarding access granted — waiting on training.",
    "ACTIVE": "Training complete — please set up Gusto for this leader.",
    "Needs Review": "Training outcome requires manual review.",
}

# Special signal returned by _check_transition to indicate a rebook is needed
_REBOOK_SIGNAL = "_rebook"


def _check_transition(page: dict, org_uri: str | None = None) -> tuple[str, str] | None:
    """Determine if a page should advance to the next pipeline stage.

    Returns (new_stage, message) or None if no transition applies.
    Special: returns (_REBOOK_SIGNAL, message) when a Fail 1 rebook is needed.
    """
    status = _get_property_value(page, "Readiness Status")

    if status == "Matched":
        compliance_val = _get_property_value(page, config.OB_COMPLIANCE_STATUS_PROPERTY)
        if _is_task_complete(compliance_val):
            # Compliance already cleared — skip straight to Onboarding Setup
            return "Onboarding Setup", _TRANSITION_MESSAGES["Onboarding Setup"]
        if _compliance_started(page):
            return "Background Check Pending", _TRANSITION_MESSAGES["Background Check Pending"]

    elif status == "Background Check Pending":
        compliance_val = _get_property_value(page, config.OB_COMPLIANCE_STATUS_PROPERTY)
        if _is_task_complete(compliance_val):
            return "Onboarding Setup", _TRANSITION_MESSAGES["Onboarding Setup"]

    elif status == "Onboarding Setup":
        if _all_access_complete(page):
            # Check if Notion Training Status is already marked complete
            training_val = _get_property_value(page, config.OB_TRAINING_STATUS_PROPERTY)
            if _is_task_complete(training_val):
                return "ACTIVE", "Returning leader — all access + training already complete."

            # For returning leaders, check Calendly training recency
            returning_val = _get_property_value(page, "Returning Leader?")
            if returning_val == "Yes" and org_uri:
                email = _get_leader_email(page)
                if email:
                    from calendly_sync import is_training_recent
                    is_recent, last_date = is_training_recent(org_uri, email)
                    if is_recent:
                        date_str = last_date.strftime("%b %d, %Y") if last_date else "recently"
                        return "ACTIVE", f"Returning leader — trained {date_str} (within {config.TRAINING_RECENCY_DAYS} days)."

            return "Training In Progress", _TRANSITION_MESSAGES["Training In Progress"]

    elif status == "Training In Progress":
        # --- Trainer Outcome routing (new process) ---
        outcome = _get_property_value(page, config.OB_TRAINING_OUTCOME_PROPERTY)

        if outcome == "Pass":
            return "ACTIVE", "Training outcome: Pass — leader is ready."
        if outcome in ("Fail 2", "No-Show"):
            return "Needs Review", f"Training outcome: {outcome} — manual review required."
        if outcome == "Fail 1":
            return _REBOOK_SIGNAL, "Training outcome: Fail 1 — rebooking training."

        # Legacy fallback: Training Status = Complete without outcome → ACTIVE
        training_val = _get_property_value(page, config.OB_TRAINING_STATUS_PROPERTY)
        if _is_task_complete(training_val) and not outcome:
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


# ---------------------------------------------------------------------------
# Ops Hub workshop lookup
# ---------------------------------------------------------------------------

def _get_leader_workshops(sheet_data: list[list[str]], leader_name: str) -> list[dict]:
    """Find all active workshop assignments for a leader from the Ops Hub sheet.

    Searches Leader 1/2/3 columns for the leader name (case-insensitive)
    and returns workshop details from each matching row.
    """
    if not sheet_data or len(sheet_data) < 2:
        return []

    header = sheet_data[0]
    col_map = {h.strip(): i for i, h in enumerate(header) if h.strip()}

    site_idx = col_map.get("Site")
    day_idx = col_map.get("Day")
    start_idx = col_map.get("Start Time")
    end_idx = col_map.get("End Time")
    lesson_idx = col_map.get("Lesson")
    district_idx = col_map.get("District")

    def _cell(row, idx):
        if idx is not None and idx < len(row):
            return row[idx].strip()
        return ""

    clean_name = leader_name.strip().lower()
    workshops = []

    for row in sheet_data[1:]:
        for col_idx in (18, 19, 20):  # Leader 1, 2, 3 columns (S, T, U)
            if col_idx < len(row) and row[col_idx].strip().lower() == clean_name:
                site = _cell(row, site_idx)
                day = _cell(row, day_idx)
                start_t = _cell(row, start_idx)
                end_t = _cell(row, end_idx)
                lesson = _cell(row, lesson_idx)
                district = _cell(row, district_idx)
                if site or lesson:
                    workshops.append({
                        "site": site,
                        "day": day,
                        "time": f"{start_t}-{end_t}" if start_t else "",
                        "lesson": lesson,
                        "district": district,
                    })
                break
    return workshops


def build_workshop_map(sheet_data: list[list[str]], leaders: list[dict]) -> dict[str, list[dict]]:
    """Build a map of leader name -> list of workshop assignments."""
    ws_map: dict[str, list[dict]] = {}
    for page in leaders:
        name = _get_leader_name(page)
        if name:
            ws_map[name] = _get_leader_workshops(sheet_data, name)
    return ws_map


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


def _send_rebook_email(name: str, email: str) -> bool:
    """Send a training rebook email with Calendly booking link.

    Returns True on success. Respects EMAILS_ENABLED kill switch.
    """
    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send rebook email to %s (%s)", name, email)
        return False

    if not email:
        log.warning("No email for %s — cannot send rebook email", name)
        return False

    import smtplib as _smtplib
    from email.mime.multipart import MIMEMultipart as _MIMEMultipart
    from email.mime.text import MIMEText as _MIMEText

    subject = "Let's Schedule Another Training Session"
    html = f"""\
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;">
<p>Hi {name},</p>
<p>We'd like to schedule another training session with you. Please use the link below
to book a time that works:</p>
<p style="text-align:center;margin:24px 0;">
  <a href="{config.CALENDLY_BOOKING_URL}"
     style="background:#4A90D9;color:#fff;padding:12px 28px;text-decoration:none;
            border-radius:6px;font-weight:bold;display:inline-block;">
    Book Training Session
  </a>
</p>
<p>If you have any questions, feel free to reply to this email.</p>
<p>Best,<br>Kodely Talent Team</p>
</body></html>"""

    msg = _MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_FROM
    msg["To"] = email
    msg.attach(_MIMEText(html, "html"))

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        with _smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
            server.starttls(context=context)
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.EMAIL_FROM, [email], msg.as_string())
        log.info("Sent rebook email to %s (%s)", name, email)
        return True
    except Exception:
        log.exception("Failed to send rebook email to %s (%s)", name, email)
        return False


def _run_transition_hooks(page: dict, new_stage: str, slack: "SlackClient") -> None:
    """Fire automation hooks when a leader transitions to a new pipeline stage.

    Called after a successful Notion status update. Each hook is wrapped in
    try/except so a failure in one doesn't block others.
    """
    name = _get_leader_name(page)
    page_id = page.get("id", "")

    if new_stage == "Onboarding Setup":
        # Leader just cleared background check → send welcome email, provision Slack,
        # mark lesson plan sent, generate trainer notes.

        # Welcome email
        try:
            we = _get_welcome_email()
            we.send_welcome_for_page(page, slack=slack)
        except Exception:
            log.exception("Hook: welcome email failed for %s", name)

        # NOTE: Slack workspace/channel invites, LearnDash, and Management Tool
        # are handled manually by the onboarding team for now.

        # Mark lesson plan as sent (auto-included in welcome email resources)
        try:
            resp = httpx.patch(
                f"{NOTION_BASE}/pages/{page_id}",
                headers=NOTION_HEADERS,
                json={"properties": {config.OB_LESSON_PLAN_PROPERTY: {"select": {"name": "Sent"}}}},
                timeout=30,
            )
            if resp.status_code < 400:
                log.info("Hook: lesson plan marked sent for %s", name)
        except Exception:
            log.exception("Hook: lesson plan mark failed for %s", name)

        # Trainer notes (AI-generated) — requires ANTHROPIC_API_KEY in .env
        if config.ANTHROPIC_API_KEY:
            try:
                tn = _get_trainer_notes()
                tn.generate_notes_for_page(page)
            except Exception:
                log.exception("Hook: trainer notes failed for %s", name)

    elif new_stage == "ACTIVE":
        # Mark Gusto as Done in Notion
        try:
            resp = httpx.patch(
                f"{NOTION_BASE}/pages/{page_id}",
                headers=NOTION_HEADERS,
                json={"properties": {config.OB_GUSTO_PROPERTY: {"select": {"name": "Done"}}}},
                timeout=30,
            )
            if resp.status_code < 400:
                log.info("Hook: Gusto marked Done for %s", name)
        except Exception:
            log.exception("Hook: Gusto mark failed for %s", name)

        # Post celebration alert
        try:
            msg = (
                f":tada: LEADER ACTIVATED\n\n"
                f"*Leader:* {name}\n\n"
                f"All onboarding steps complete — please add to Gusto."
            )
            post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
        except Exception:
            log.exception("Hook: ACTIVE celebration alert failed for %s", name)

    elif new_stage == "Needs Review":
        # Post review alert
        outcome = _get_property_value(page, config.OB_TRAINING_OUTCOME_PROPERTY)
        try:
            msg = (
                f":rotating_light: NEEDS REVIEW\n\n"
                f"*Leader:* {name}\n"
                f"*Training Outcome:* {outcome}\n\n"
                f"Manual review required."
            )
            post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
        except Exception:
            log.exception("Hook: Needs Review alert failed for %s", name)


def _process_one_transition(
    page: dict,
    state: dict,
    slack: "SlackClient",
    dry_run: bool,
    sheet_data: list[list[str]] | None,
    creds: ServiceCredentials | None,
    org_uri: str | None,
    messages: list[str],
) -> bool:
    """Process a single pipeline transition for one leader.

    Returns True if a transition happened (for fast-advance re-evaluation).
    """
    page_id = page.get("id", "")
    name = _get_leader_name(page)
    region = _get_region(page)

    if not name:
        return False

    # --- Issue 4: Missing email check ---
    current_stage = _get_property_value(page, "Readiness Status")
    if current_stage in ("Background Check Pending", "Onboarding Setup", "Training In Progress"):
        email = _get_leader_email(page)
        if not email:
            missing_key = f"missing_email_{page_id}"
            if not state.get(missing_key):
                if dry_run:
                    print(f"--- DRY RUN: MISSING EMAIL ---")
                    print(f"  :warning: {name} ({current_stage}) — no email on file")
                    print(f"  Would post alert and skip advancement")
                    print()
                else:
                    try:
                        msg = (
                            f":warning: MISSING EMAIL\n\n"
                            f"*Leader:* {name}\n"
                            f"*Stage:* {current_stage}\n\n"
                            f"No email on file — cannot advance pipeline. Please add email to Notion card."
                        )
                        post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
                    except Exception:
                        log.exception("Failed to post missing email alert for %s", name)
                state[missing_key] = True
            return False

    result = _check_transition(page, org_uri=org_uri)
    if result is None:
        return False

    new_stage, reason = result

    # --- Handle rebook (Fail 1) ---
    if new_stage == _REBOOK_SIGNAL:
        rebook_key = f"rebook_{page_id}"
        if state.get(rebook_key):
            return False

        email = _get_leader_email(page)
        if dry_run:
            print(f"--- DRY RUN: TRAINING REBOOK ---")
            print(f"  {name} ({region}): Fail 1 — would clear trainer & rebook")
            print(f"  Would send rebook email to {email or '(no email)'}")
            print()
        else:
            # Clear Trainer Assigned, Training Status, Training Outcome
            try:
                httpx.patch(
                    f"{NOTION_BASE}/pages/{page_id}",
                    headers=NOTION_HEADERS,
                    json={"properties": {
                        "Trainer Assigned": {"select": None},
                        config.OB_TRAINING_STATUS_PROPERTY: {"select": None},
                        config.OB_TRAINING_OUTCOME_PROPERTY: {"select": None},
                    }},
                    timeout=30,
                )
                log.info("Rebook: cleared trainer/training for %s", name)
            except Exception:
                log.exception("Rebook: failed to clear Notion properties for %s", name)

            # Send rebook email
            if email:
                _send_rebook_email(name, email)

            # Post Slack alert
            try:
                msg = (
                    f":arrows_counterclockwise: TRAINING REBOOK\n\n"
                    f"*Leader:* {name}\n"
                    f"*Outcome:* Fail 1 — trainer cleared, booking link re-sent.\n"
                    f"Leader will rebook training via Calendly."
                )
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
            except Exception:
                log.exception("Failed to post rebook alert for %s", name)

            messages.append(f"{name}: Fail 1 → rebook")

        state[rebook_key] = True
        return False  # Not a stage transition, so no fast-advance

    # --- Normal pipeline advance ---
    pipeline_key = f"pipeline_{page_id}_{new_stage}"
    if state.get(pipeline_key):
        return False

    if dry_run:
        print(f"--- DRY RUN: PIPELINE ADVANCE ---")
        print(f"  {name} ({region}): {current_stage} → {new_stage}")
        print(f"  Reason: {reason}")
        if sheet_data:
            workshops = _get_leader_workshops(sheet_data, name)
            if workshops:
                print(f"  Workshop(s):")
                for ws in workshops:
                    parts = [ws.get("site", ""), ws.get("day", ""), ws.get("time", ""), ws.get("lesson", "")]
                    print(f"    • {' | '.join(p for p in parts if p)}")
            else:
                print(f"  Workshop(s): NONE found in Ops Hub")
        color = _STAGE_COLOR_MAP.get(new_stage)
        if color and sheet_data:
            cells = _find_leader_cells(sheet_data, name)
            print(f"  Color sync: would update {len(cells)} cell(s) to {new_stage} color")
        print()
    else:
        if not _patch_readiness_status(page_id, new_stage):
            return False

        # Sync Ops Hub cell color if this transition has a color mapping
        color = _STAGE_COLOR_MAP.get(new_stage)
        if color and sheet_data and creds:
            cells = _find_leader_cells(sheet_data, name)
            for r, c in cells:
                try:
                    _update_cell_color(creds, r, c, color)
                except Exception:
                    log.exception("Failed to update cell color for %s at (%d,%d)", name, r, c)

        # Look up workshop assignments so the team knows what to onboard for
        workshop_lines = ""
        if sheet_data:
            workshops = _get_leader_workshops(sheet_data, name)
            if workshops:
                workshop_lines = "\n\n\U0001f3eb Workshop Assignment(s):\n"
                for ws in workshops:
                    parts = []
                    if ws.get("site"):
                        parts.append(ws["site"])
                    if ws.get("day"):
                        parts.append(ws["day"])
                    if ws.get("time"):
                        parts.append(ws["time"])
                    if ws.get("lesson"):
                        parts.append(f"Lesson: {ws['lesson']}")
                    if ws.get("district"):
                        parts.append(f"({ws['district']})")
                    workshop_lines += f"  • {' | '.join(parts)}\n"
            else:
                workshop_lines = "\n\n\u26a0\ufe0f No workshop assignment found in Ops Hub yet."

        msg = (
            f"\U0001f4ca PIPELINE UPDATE\n\n"
            f"Leader: {name} \u2192 {new_stage}\n"
            f"Region: {region}\n"
            f"{reason}"
            f"{workshop_lines}"
        )
        try:
            post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
            log.info("Pipeline: %s moved to %s", name, new_stage)
        except Exception:
            log.exception("Failed to post pipeline update for %s", name)

        # --- Automation hooks on transition ---
        _run_transition_hooks(page, new_stage, slack)

        messages.append(f"{name}: {current_stage} → {new_stage}")

    state[pipeline_key] = True
    return True


def advance_pipeline(
    leaders: list[dict],
    state: dict,
    slack: "SlackClient",
    dry_run: bool = False,
    sheet_data: list[list[str]] | None = None,
    creds: ServiceCredentials | None = None,
    org_uri: str | None = None,
) -> list[str]:
    """Check each leader for pipeline transitions and advance if conditions are met.

    Returns list of transition messages. Updates state dict to track notified transitions.
    Includes fast-advance: after all transitions, re-queries Notion and runs a second pass
    so cards can move through multiple stages in one run.
    """
    messages = []

    for page in leaders:
        _process_one_transition(page, state, slack, dry_run, sheet_data, creds, org_uri, messages)

    # --- Part C: Pipeline fast-advance (second pass) ---
    if not dry_run and messages:
        log.info("Fast-advance: re-querying Notion for second pass...")
        fresh_leaders = _dedup_leaders(query_onboarding_leaders())
        second_pass_count = 0
        for page in fresh_leaders:
            if _process_one_transition(page, state, slack, dry_run, sheet_data, creds, org_uri, messages):
                second_pass_count += 1
        if second_pass_count:
            log.info("Fast-advance: %d additional transition(s) in second pass", second_pass_count)

    return messages


def catch_up_hooks(
    leaders: list[dict],
    state: dict,
    slack: "SlackClient",
    dry_run: bool = False,
) -> list[str]:
    """Fire transition hooks for cards that were manually moved to a new stage.

    If someone manually moves a card to 'Onboarding Setup' in Notion, the
    welcome email / lesson plan hooks won't fire because advance_pipeline()
    didn't trigger the transition.  This function detects those cases by
    checking for cards in 'Onboarding Setup' where 'Onboarding Email Sent?'
    is not yet marked complete, and fires the hooks.
    """
    messages = []

    for page in leaders:
        page_id = page.get("id", "")
        name = _get_leader_name(page)
        if not name:
            continue

        status = _get_property_value(page, "Readiness Status")
        if status != "Onboarding Setup":
            continue

        email_sent = _get_property_value(page, config.OB_ONBOARDING_EMAIL_PROPERTY)
        if _is_task_complete(email_sent):
            continue

        # This card is in Onboarding Setup but hasn't received a welcome email
        hook_key = f"catchup_hooks_{page_id}_Onboarding Setup"
        if state.get(hook_key):
            continue

        if dry_run:
            print(f"--- DRY RUN: CATCH-UP HOOKS ---")
            print(f"  {name}: in Onboarding Setup but welcome email not sent")
            print(f"  Would fire: welcome email, lesson plan mark")
            print()
        else:
            log.info("Catch-up hooks for %s (manually moved to Onboarding Setup)", name)
            _run_transition_hooks(page, "Onboarding Setup", slack)
            messages.append(f"{name}: catch-up hooks fired (welcome email + lesson plan)")

        state[hook_key] = True

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

def build_digest_message(leaders: list[dict], ws_map: dict[str, list[dict]] | None = None) -> str:
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
        parts = [
            f"> {entry['name']} \u2014 {entry['region']}{start_str}",
            f"> \u2705 {done_str}",
            f"> \u274c {todo_str}",
        ]
        # Workshop assignments from Ops Hub
        workshops = (ws_map or {}).get(entry["name"], [])
        if workshops:
            ws_lines = []
            for w in workshops:
                ws_lines.append(f">  \U0001f4cd {w['site']} \u2014 {w['lesson']} \u2014 {w['day']} {w['time']}")
            parts.extend(ws_lines)
        else:
            parts.append(">  \U0001f4cd _No workshop assigned_")
        return "\n".join(parts)

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
# Automation 1b: Onboarding Digest — HTML Email
# ---------------------------------------------------------------------------

def build_digest_email_html(leaders: list[dict], ws_map: dict[str, list[dict]] | None = None) -> str:
    """Build the Daily Onboarding Report as an HTML email.

    Same data as build_digest_message() but rendered as colour-coded HTML tables.
    """
    today = date.today()
    today_str = today.strftime("%b %d, %Y")
    urgent, warning, in_progress, fully_done_names = [], [], [], []

    for page in leaders:
        name = _get_leader_name(page)
        region = _get_region(page)
        start = _get_start_date(page)
        completed = _get_completed_tasks(page)
        incomplete = _get_incomplete_tasks(page)

        if not incomplete:
            fully_done_names.append(name)
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

    for group in (urgent, warning, in_progress):
        group.sort(key=lambda e: e["start"] or date.max)

    total = len(leaders)

    def _task_badges(completed: list[str], incomplete: list[str]) -> str:
        parts = []
        for t in completed:
            parts.append(f"<span style='color:green;'>&#9989; {t}</span>")
        for t in incomplete:
            parts.append(f"<span style='color:red;'>&#10060; {t}</span>")
        return " &nbsp; ".join(parts)

    def _workshop_cell(name: str) -> str:
        workshops = (ws_map or {}).get(name, [])
        if not workshops:
            return "<span style='color:#888;'>None assigned</span>"
        parts = []
        for w in workshops:
            parts.append(f"{w['site']} &mdash; {w['lesson']}<br><small>{w['day']} {w['time']}</small>")
        return "<br>".join(parts)

    def _table(entries: list[dict]) -> str:
        rows = [
            "<table style='border-collapse:collapse;width:100%;' border='1' cellpadding='6' cellspacing='0'>",
            "<tr style='background:#eee;'><th>Leader</th><th>Region</th><th>Starts</th><th>Tasks</th><th>Workshop(s)</th></tr>",
        ]
        for e in entries:
            if e["start"]:
                d = e["days_until"]
                if d < 0:
                    start_str = f"{e['start'].strftime('%b %-d')} ({-d}d ago)"
                else:
                    start_str = f"{e['start'].strftime('%b %-d')} ({d}d)"
            else:
                start_str = "TBD"
            rows.append(
                f"<tr><td>{e['name']}</td>"
                f"<td>{e['region']}</td>"
                f"<td>{start_str}</td>"
                f"<td>{_task_badges(e['completed'], e['incomplete'])}</td>"
                f"<td>{_workshop_cell(e['name'])}</td></tr>"
            )
        rows.append("</table>")
        return "\n".join(rows)

    html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'></head><body style='font-family:Arial,sans-serif;max-width:750px;margin:auto;'>",
        f"<h2 style='background:#1a1a2e;color:#fff;padding:14px 18px;margin:0;border-radius:6px 6px 0 0;'>KODELY DAILY ONBOARDING REPORT &mdash; {today_str}</h2>",
        f"<p style='padding:0 18px;'><strong>{total}</strong> leader{'s' if total != 1 else ''} actively onboarding</p>",
    ]

    if urgent:
        html_parts.append(f"<h3 style='color:#cc0000;margin:18px 0 8px;'>&#128680; URGENT &mdash; Starting in &lt;{config.OB_URGENT_DAYS} days</h3>")
        html_parts.append(_table(urgent))

    if warning:
        html_parts.append(f"<h3 style='color:#cc8800;margin:18px 0 8px;'>&#9888;&#65039; WARNING &mdash; Starting in &lt;{config.OB_WARNING_DAYS} days</h3>")
        html_parts.append(_table(warning))

    if in_progress:
        html_parts.append("<h3 style='margin:18px 0 8px;'>&#9203; IN PROGRESS</h3>")
        html_parts.append(_table(in_progress))

    if fully_done_names:
        names_str = ", ".join(fully_done_names)
        html_parts.append(
            f"<p style='margin:18px 0;'>&#9989; <strong>{len(fully_done_names)}</strong> "
            f"leader{'s' if len(fully_done_names) != 1 else ''} fully onboarded: {names_str}</p>"
        )

    html_parts.append("<br><p style='color:#999;font-size:12px;'>Generated by Kodely Onboarding Report</p>")
    html_parts.append("</body></html>")
    return "\n".join(html_parts)


def build_detailed_onboarding_report(
    leaders: list[dict],
    ws_map: dict[str, list[dict]] | None = None,
) -> str:
    """Build a detailed HTML report with individual columns for each onboarding step.

    Groups leaders by pipeline stage and shows per-task status with clear
    auto vs manual labels so the team knows exactly what to action.
    """
    today = date.today()
    today_str = today.strftime("%b %d, %Y")

    # Task columns: (notion_property, display_name, is_automated)
    STEP_COLS = [
        (config.OB_COMPLIANCE_STATUS_PROPERTY, "Background Check", True),
        (config.OB_ONBOARDING_EMAIL_PROPERTY, "Welcome Email", True),
        (config.OB_LESSON_PLAN_PROPERTY, "Lesson Plan", True),
        (config.OB_SLACK_INVITE_PROPERTY, "Slack Invite", False),
        (config.OB_WORKSHOP_SLACK_PROPERTY, "Workshop Slack", False),
        (config.OB_GUSTO_PROPERTY, "Gusto", False),
        (config.OB_TRAINING_STATUS_PROPERTY, "Training", True),
        (config.OB_TRAINING_OUTCOME_PROPERTY, "Outcome", True),
    ]

    # Group leaders by pipeline stage
    stage_order = [
        "Matched",
        "Background Check Pending",
        "Onboarding Setup",
        "Training In Progress",
        "ACTIVE",
        "Needs Review",
    ]
    by_stage: dict[str, list[dict]] = {s: [] for s in stage_order}
    other_stage: list[dict] = []

    for page in leaders:
        status = _get_property_value(page, "Readiness Status")
        if status in by_stage:
            by_stage[status].append(page)
        else:
            other_stage.append(page)

    # Count totals
    total = len(leaders)
    active_count = len(by_stage.get("ACTIVE", []))
    pipeline_count = total - active_count

    # --- Build HTML ---
    html = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'></head>",
        "<body style='font-family:Arial,sans-serif;max-width:1000px;margin:auto;'>",
        f"<h2 style='background:#1a1a2e;color:#fff;padding:14px 18px;margin:0;"
        f"border-radius:6px 6px 0 0;'>KODELY ONBOARDING DETAILED REPORT &mdash; {today_str}</h2>",
        f"<p style='padding:4px 18px;margin:0;'><strong>{pipeline_count}</strong> leaders in pipeline"
        f" &nbsp;|&nbsp; <strong>{active_count}</strong> active</p>",
        "<p style='padding:0 18px;margin:4px 0 12px;font-size:13px;color:#555;'>"
        "&#9889; = Automated &nbsp;&nbsp; &#9997; = Manual action needed</p>",
    ]

    # Column header row builder
    def _header_row() -> str:
        cols = ["<th style='padding:6px 8px;text-align:left;white-space:nowrap;'>Leader</th>",
                "<th style='padding:6px 8px;'>Region</th>",
                "<th style='padding:6px 8px;'>Starts</th>",
                "<th style='padding:6px 8px;'>Workshop</th>"]
        for _, display, auto in STEP_COLS:
            icon = "&#9889;" if auto else "&#9997;"
            cols.append(
                f"<th style='padding:6px 4px;font-size:12px;text-align:center;"
                f"white-space:nowrap;'>{icon}<br>{display}</th>"
            )
        return "<tr style='background:#eee;'>" + "".join(cols) + "</tr>"

    def _status_cell(value: str) -> str:
        """Render a single task status as a colored cell."""
        if _is_task_complete(value):
            return ("<td style='text-align:center;background:#d4edda;color:#155724;"
                    "font-weight:bold;padding:4px;'>&#9989;</td>")
        if value and value.lower() not in ("not sent", "no", "n/a", ""):
            # In progress (has a value but not complete)
            short = value[:15] + ".." if len(value) > 17 else value
            return (f"<td style='text-align:center;background:#fff3cd;color:#856404;"
                    f"padding:4px;font-size:11px;'>{short}</td>")
        return ("<td style='text-align:center;background:#f8d7da;color:#721c24;"
                "padding:4px;'>&#10060;</td>")

    def _leader_row(page: dict) -> str:
        name = _get_leader_name(page)
        region = _get_region(page)
        start = _get_start_date(page)
        days_until = (start - today).days if start else 999

        # Urgency coloring
        if days_until < 0:
            start_str = f"{start.strftime('%b %-d')}<br><small style='color:red;'>({-days_until}d ago)</small>"
            row_bg = "#fff0f0"
        elif days_until < config.OB_URGENT_DAYS:
            start_str = f"{start.strftime('%b %-d')}<br><small style='color:red;'>({days_until}d)</small>"
            row_bg = "#fff0f0"
        elif days_until < config.OB_WARNING_DAYS:
            start_str = f"{start.strftime('%b %-d')}<br><small style='color:#856404;'>({days_until}d)</small>"
            row_bg = "#fffbe6"
        elif start:
            start_str = f"{start.strftime('%b %-d')}<br><small>({days_until}d)</small>"
            row_bg = "#fff"
        else:
            start_str = "TBD"
            row_bg = "#fff"

        # Workshop
        workshops = (ws_map or {}).get(name, [])
        if workshops:
            ws_parts = []
            for w in workshops:
                ws_parts.append(f"<small>{w['site']}<br>{w['day']} {w['time']}</small>")
            ws_cell = "<br>".join(ws_parts)
        else:
            ws_cell = "<small style='color:#999;'>None</small>"

        # Task cells
        task_cells = []
        for prop_name, _, _ in STEP_COLS:
            val = _get_property_value(page, prop_name)
            task_cells.append(_status_cell(val))

        return (
            f"<tr style='background:{row_bg};'>"
            f"<td style='padding:6px 8px;white-space:nowrap;'><strong>{name}</strong></td>"
            f"<td style='padding:6px 8px;text-align:center;'>{region}</td>"
            f"<td style='padding:6px 8px;text-align:center;'>{start_str}</td>"
            f"<td style='padding:6px 8px;'>{ws_cell}</td>"
            + "".join(task_cells) +
            "</tr>"
        )

    # Render each stage section
    stage_labels = {
        "Matched": ("&#127920; MATCHED", "#e8f0fe", "Waiting for background check to be sent"),
        "Background Check Pending": ("&#128270; BACKGROUND CHECK PENDING", "#f3e5f5", "Checkr running — waiting for clearance"),
        "Onboarding Setup": ("&#128736; ONBOARDING SETUP", "#fff8e1", "Access setup in progress — check manual tasks"),
        "Training In Progress": ("&#127891; TRAINING IN PROGRESS", "#e8f5e9", "Waiting for training completion via Calendly"),
        "ACTIVE": ("&#9989; ACTIVE", "#e8f5e9", "Fully onboarded and active"),
        "Needs Review": ("&#128680; NEEDS REVIEW", "#fce4ec", "Training failed or no-show — manual review required"),
    }

    for stage in stage_order:
        pages = by_stage.get(stage, [])
        if not pages:
            continue

        label, bg, desc = stage_labels.get(stage, (stage, "#f5f5f5", ""))
        # Sort by start date
        pages.sort(key=lambda p: _get_start_date(p) or date.max)

        # Count pending manual tasks in this stage
        manual_pending = 0
        for p in pages:
            for prop_name, _, is_auto in STEP_COLS:
                if not is_auto and not _is_task_complete(_get_property_value(p, prop_name)):
                    manual_pending += 1

        manual_note = f" &nbsp;|&nbsp; <strong style='color:#cc0000;'>{manual_pending} manual task(s) pending</strong>" if manual_pending else ""

        html.append(
            f"<h3 style='background:{bg};padding:10px 14px;margin:18px 0 0;border-radius:4px;'>"
            f"{label} &mdash; {len(pages)} leader{'s' if len(pages) != 1 else ''}"
            f"{manual_note}</h3>"
        )
        html.append(f"<p style='margin:2px 0 8px 14px;font-size:12px;color:#666;'>{desc}</p>")
        html.append(
            "<table style='border-collapse:collapse;width:100%;font-size:13px;' border='1' cellpadding='0' cellspacing='0'>"
        )
        html.append(_header_row())
        for p in pages:
            html.append(_leader_row(p))
        html.append("</table>")

    if other_stage:
        html.append(f"<p style='margin:18px 0;color:#888;'>{len(other_stage)} leader(s) in other stages (not shown)</p>")

    # Legend
    html.append(
        "<div style='margin:20px 0;padding:12px;background:#f5f5f5;border-radius:4px;font-size:12px;'>"
        "<strong>Legend:</strong><br>"
        "&#9989; = Complete &nbsp;&nbsp; &#10060; = Not started &nbsp;&nbsp; "
        "<span style='background:#fff3cd;padding:2px 6px;'>In progress</span> = Started but not done<br>"
        "&#9889; = Auto-handled by system &nbsp;&nbsp; &#9997; = Needs manual team action"
        "</div>"
    )

    html.append("<p style='color:#999;font-size:11px;'>Generated by Kodely Onboarding Automation</p>")
    html.append("</body></html>")
    return "\n".join(html)


def send_digest_email(html: str, subject: str) -> None:
    """Send the onboarding digest email via SMTP (same pattern as email_digest.py)."""
    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send onboarding digest '%s'", subject)
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

    log.info("Onboarding digest email sent to %s (cc: %s)",
             ", ".join(to_addrs), ", ".join(cc_addrs) or "none")


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
    parser.add_argument("--email", action="store_true", help="Send HTML email in addition to Slack")
    parser.add_argument("--email-only", action="store_true", help="Send HTML email only (no Slack)")
    args = parser.parse_args()

    run_digest = not args.compliance_only
    run_compliance = not args.digest_only
    send_slack = not args.email_only
    send_html = args.email or args.email_only

    slack = SlackClient(token=config.SLACK_BOT_TOKEN)
    leaders = _dedup_leaders(query_onboarding_leaders())

    if not leaders:
        log.info("No onboarding leaders found. Exiting.")
        return

    # --- Load Google Sheets for workshop lookup + color sync ---
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    gs_creds = ServiceCredentials.from_service_account_file(
        config.GOOGLE_CREDENTIALS_PATH, scopes=scopes
    )
    gc = gspread.authorize(gs_creds)
    spreadsheet = gc.open_by_key(config.GOOGLE_SHEET_ID)
    sheet = _get_worksheet_by_gid(spreadsheet, config.SHEET_GID)
    sheet_data = sheet.get_all_values()
    log.info("Loaded %d rows from Ops Hub", len(sheet_data))

    ws_map = build_workshop_map(sheet_data, leaders)

    # --- Daily Digest ---
    if run_digest:
        log.info("Building daily digest for %d leaders...", len(leaders))

        if send_slack:
            digest_msg = build_digest_message(leaders, ws_map=ws_map)
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

        if send_html:
            subject = f"Kodely Onboarding Report — {date.today().strftime('%b %d, %Y')}"
            html = build_digest_email_html(leaders, ws_map=ws_map)
            if args.dry_run:
                print("--- DRY RUN: ONBOARDING EMAIL ---")
                print(f"Subject: {subject}")
                print(html)
                print()
            else:
                try:
                    send_digest_email(html, subject)
                except Exception:
                    log.exception("Failed to send onboarding digest email")

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

        # --- Pipeline Auto-Advance ---
        log.info("Checking pipeline transitions for %d leaders...", len(leaders))

        # Fetch Calendly org URI for training recency checks
        org_uri = None
        try:
            from calendly_sync import get_current_user
            user = get_current_user()
            org_uri = user["current_organization"]
        except Exception:
            log.warning("Could not connect to Calendly — training recency checks will be skipped")

        transitions = advance_pipeline(
            leaders, digest_state, slack,
            dry_run=args.dry_run,
            sheet_data=sheet_data,
            creds=gs_creds,
            org_uri=org_uri,
        )
        if transitions:
            log.info("Advanced %d leader(s) in pipeline: %s", len(transitions), "; ".join(transitions))
        else:
            log.info("No pipeline transitions.")

        # --- Catch-up hooks for manually moved cards ---
        catchups = catch_up_hooks(leaders, digest_state, slack, dry_run=args.dry_run)
        if catchups:
            log.info("Catch-up hooks fired for %d leader(s): %s", len(catchups), "; ".join(catchups))

        if not args.dry_run:
            save_digest_state(digest_state)
            log.info("Digest state saved to %s", config.DIGEST_STATE_PATH)

    log.info("Done%s.", " (dry run)" if args.dry_run else "")


if __name__ == "__main__":
    main()
