#!/usr/bin/env python3
"""
Checkr Background Check Automation

Finds leaders with Compliance Status = "Not Sent", creates a Checkr candidate
and invitation, then polls pending invitations. When Checkr clears, sets
Compliance Status to "Cleared" on Notion and posts Slack alerts.

Usage:
  python checkr_sync.py --dry-run   # print what would happen
  python checkr_sync.py             # run for real
"""

import argparse
import json
import logging
import os
import re
import smtplib
import ssl
import time
from datetime import datetime, timezone, timedelta

import certifi
import gspread
import httpx
from google.oauth2.service_account import Credentials as ServiceCredentials
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

CHECKR_BASE = "https://api.checkr.com/v1"

# Region → US state mapping for Checkr work_locations.
# Keys are lowercased, stripped. Covers all 70 Notion Region select options.
REGION_TO_STATE: dict[str, str] = {
    # California
    "sf": "CA", "san francisco": "CA", "la": "CA", "oakland": "CA",
    "east bay": "CA", "san jose": "CA", "san mateo": "CA", "sunnyvale": "CA",
    "menlo park": "CA", "menio park": "CA", "menlo park/palo alto": "CA",
    "redwood city": "CA", "mountain view": "CA", "los gatos": "CA",
    "hillsborough": "CA", "pleasant hill": "CA", "berkley": "CA",
    "marin": "CA", "marin county": "CA", "petaluma ca": "CA",
    "napa/santa rosa": "CA", "santa rosa": "CA", "monterey": "CA",
    "sacramento": "CA", "twin rivers": "CA", "sunnyvale/san jose": "CA",
    "sunnyvale/palo alto": "CA", "san mateo/ hillsborough san francisco": "CA",
    "bakersfield": "CA", "mcfarland": "CA", "san joaquin valley": "CA",
    "pasadena": "CA", "inglewood": "CA", "pomona": "CA", "anaheim": "CA",
    "ventura": "CA", "san diego": "CA", "vista": "CA",
    "encinitas/ carlsbad": "CA", "california": "CA",
    "park century school": "CA",  # LA-area school
    "orchard": "CA",
    # New York
    "nyc": "NY", "manhattan": "NY", "brooklyn": "NY",
    "queens": "NY", "quieens": "NY",
    # Texas
    "austin": "TX", "round rock": "TX",
    # Virginia / DC area
    "virginia": "VA", "virgi": "VA", "fairfax": "VA",
    "washington dc": "DC", "maryland": "MD",
    # Colorado
    "colorado": "CO", "denver": "CO",
    # Illinois
    "illinois": "IL", "chicago": "IL", "woodridge": "IL",
    # Florida
    "miami": "FL", "tampa": "FL",
    # Arizona
    "arizona": "AZ", "gilbert az": "AZ",
    # Massachusetts
    "massachusetts": "MA", "brighton and cambridge massachusetts.": "MA",
    "shrewsbury": "MA",
    # Michigan
    "detroit": "MI",
    # Minnesota
    "minnesota/minniapolis": "MN",
    # Washington
    "seattle": "WA", "seat": "WA",
}


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.CHECKR_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.CHECKR_STATE_PATH, "w") as f:
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
    if prop_type == "status":
        st = prop.get("status")
        return st.get("name", "") if st else ""
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in parts)
    if prop_type == "email":
        return prop.get("email", "") or ""
    if prop_type == "rich_text":
        parts = prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _resolve_work_state(page: dict) -> str:
    """Map a leader's Notion Region to a US state code for Checkr."""
    region = _get_property_value(page, "Region").strip().lower()
    state = REGION_TO_STATE.get(region)
    if state:
        return state
    # If region not in mapping, log a warning and default to CA
    if region:
        log.warning("Unknown region '%s' — defaulting Checkr work_location to CA", region)
    return "CA"


def _get_leader_name(page: dict) -> str:
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_email(page: dict) -> str:
    """Extract email from either Email property or rich_text field."""
    props = page.get("properties", {})
    # Try email type first
    email_prop = props.get("Email", {})
    if email_prop.get("type") == "email" and email_prop.get("email"):
        return email_prop["email"]
    # Fallback to rich_text
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


def query_not_sent_leaders() -> list[dict]:
    """Query Notion for leaders with Compliance Status = 'Not Sent'."""
    body = {
        "filter": {
            "and": [
                {
                    "or": [
                        {"property": "Readiness Status", "select": {"equals": "Matched"}},
                        {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                        {"property": "Readiness Status", "select": {"equals": "Background Check Pending"}},
                        {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
                    ],
                },
                {"property": config.OB_COMPLIANCE_STATUS_PROPERTY, "select": {"equals": "Not Sent"}},
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


def query_pending_leaders() -> list[dict]:
    """Query Notion for leaders with Compliance Status = 'Sent' (pending Checkr result)."""
    body = {
        "filter": {
            "property": config.OB_COMPLIANCE_STATUS_PROPERTY,
            "select": {"equals": "Sent"},
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


def _patch_compliance_status(page_id: str, status: str) -> bool:
    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {config.OB_COMPLIANCE_STATUS_PROPERTY: {"select": {"name": status}}}},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Failed to patch Compliance Status for %s: %s %s", page_id, resp.status_code, resp.text)
        return False
    return True


# ---------------------------------------------------------------------------
# Form Responses email lookup
# ---------------------------------------------------------------------------

def load_form_emails() -> dict[str, str]:
    """Load name -> email mapping from the Leader Confirmation Form Responses sheet.

    Returns {lowercase_name: email}. Most recent form entry wins per name.
    """
    creds = ServiceCredentials.from_service_account_file(
        config.GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(config.FORM_SHEET_ID)

    sheet = None
    for ws in ss.worksheets():
        if ws.id == config.FORM_SHEET_GID:
            sheet = ws
            break

    if not sheet:
        log.warning("Form Responses sheet not found (GID %s)", config.FORM_SHEET_GID)
        return {}

    rows = sheet.get_all_values()
    if len(rows) < 2:
        return {}

    name_col = None
    email_col = None
    for i, h in enumerate(rows[0]):
        h_lower = h.strip().lower()
        if "full legal name" in h_lower:
            name_col = i
        elif "email" in h_lower and "personal" in h_lower:
            email_col = i

    if name_col is None or email_col is None:
        log.warning("Could not find name/email columns in Form Responses")
        return {}

    mapping: dict[str, str] = {}
    for row in rows[1:]:
        name = row[name_col].strip() if name_col < len(row) else ""
        email = row[email_col].strip() if email_col < len(row) else ""
        if name and email:
            mapping[name.lower()] = email

    log.info("Loaded %d name-email pairs from Form Responses", len(mapping))
    return mapping


def load_form_minors() -> set[str]:
    """Load names of leaders who are under 18 from the Form Responses sheet.

    Checks two columns:
      - "Do you require a work permit? (under 18)" → "yes" means minor
      - "Are you currently over the age of 18?" → "no" means minor

    Returns a set of lowercase names.
    """
    creds = ServiceCredentials.from_service_account_file(
        config.GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(config.FORM_SHEET_ID)

    sheet = None
    for ws in ss.worksheets():
        if ws.id == config.FORM_SHEET_GID:
            sheet = ws
            break

    if not sheet:
        return set()

    rows = sheet.get_all_values()
    if len(rows) < 2:
        return set()

    name_col = None
    work_permit_col = None
    over_18_col = None
    for i, h in enumerate(rows[0]):
        h_lower = h.strip().lower()
        if "full legal name" in h_lower:
            name_col = i
        elif "work permit" in h_lower and "under 18" in h_lower:
            work_permit_col = i
        elif "over the age of 18" in h_lower:
            over_18_col = i

    if name_col is None:
        return set()

    minors: set[str] = set()
    for row in rows[1:]:
        name = row[name_col].strip().lower() if name_col < len(row) else ""
        if not name:
            continue
        wp = row[work_permit_col].strip().lower() if work_permit_col is not None and work_permit_col < len(row) else ""
        o18 = row[over_18_col].strip().lower() if over_18_col is not None and over_18_col < len(row) else ""
        if wp == "yes" or o18 == "no":
            minors.add(name)

    log.info("Found %d minor (under-18) leader(s) in Form Responses", len(minors))
    return minors


def _resolve_email(page: dict, form_emails: dict[str, str]) -> str:
    """Get leader's email: try Notion Email property, then form lookup, then title fallback."""
    email = _get_email(page)
    if email:
        return email

    name = _get_leader_name(page).strip()
    clean = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', name).strip().lower()
    clean = re.sub(r'\s*\([^)]*\)\s*$', '', clean).strip()

    # Exact match
    if clean in form_emails:
        return form_emails[clean]

    # First + last name match
    parts = clean.split()
    if len(parts) >= 2:
        for fn, em in form_emails.items():
            fp = fn.split()
            if len(fp) >= 2 and fp[0] == parts[0] and fp[-1] == parts[-1]:
                return em

    return ""


# ---------------------------------------------------------------------------
# Checkr history check (skip if clear within 1 year)
# ---------------------------------------------------------------------------

def _load_checkr_name_index() -> dict[str, list[dict]]:
    """Load all Checkr candidates and index by lowercase full name.

    Returns {lowercase_name: [candidate_dicts]}.
    Cached for the duration of the run.
    """
    if hasattr(_load_checkr_name_index, "_cache"):
        return _load_checkr_name_index._cache

    index: dict[str, list[dict]] = {}
    page = 1
    total = 0
    while page <= 20:
        resp = httpx.get(
            f"{CHECKR_BASE}/candidates",
            auth=_checkr_auth(),
            params={"per_page": 100, "page": page},
            timeout=30,
        )
        if resp.status_code >= 400:
            break
        data = resp.json().get("data", [])
        if not data:
            break
        for c in data:
            fn = (c.get("first_name") or "").strip().lower()
            ln = (c.get("last_name") or "").strip().lower()
            full = f"{fn} {ln}".strip()
            if full:
                index.setdefault(full, []).append(c)
        total += len(data)
        page += 1

    log.info("Indexed %d Checkr candidates (%d unique names)", total, len(index))
    _load_checkr_name_index._cache = index
    return index


def _check_candidate_reports(candidates: list[dict]) -> str | None:
    """Check a list of Checkr candidates for a clear report within the last year."""
    one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
    for c in candidates:
        for rid in c.get("report_ids", []):
            rr = httpx.get(f"{CHECKR_BASE}/reports/{rid}", auth=_checkr_auth(), timeout=30)
            if rr.status_code >= 400:
                continue
            report = rr.json()
            if report.get("result") != "clear":
                continue
            completed = report.get("completed_at", "")
            if not completed:
                continue
            try:
                completed_dt = datetime.fromisoformat(completed.replace("Z", "+00:00"))
                if completed_dt > one_year_ago:
                    return "clear"
            except ValueError:
                continue
    return None


def check_existing_checkr(email: str, name: str = "") -> str | None:
    """Search Checkr for an existing clear report by email OR name.

    1. Search by email (exact match, fast)
    2. If not found, search the full candidate index by name (catches
       cases where the leader used a different email for Checkr)

    Returns 'clear' if found with a clear report within 1 year, else None.
    """
    # --- Pass 1: search by email ---
    resp = httpx.get(
        f"{CHECKR_BASE}/candidates",
        auth=_checkr_auth(),
        params={"email": email},
        timeout=30,
    )
    if resp.status_code < 400:
        candidates = resp.json().get("data", [])
        result = _check_candidate_reports(candidates)
        if result:
            return result

    # --- Pass 2: search by name in full index ---
    if name:
        clean_name = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', name).replace('\n', ' ').strip().lower()
        clean_name = re.sub(r'\s*\([^)]*\)\s*$', '', clean_name).strip()
        if clean_name:
            index = _load_checkr_name_index()
            name_matches = index.get(clean_name, [])
            if name_matches:
                result = _check_candidate_reports(name_matches)
                if result:
                    log.info("Found clear Checkr via name match for '%s' (email mismatch)", clean_name)
                    return result

    return None


# ---------------------------------------------------------------------------
# Checkr API helpers
# ---------------------------------------------------------------------------

def _checkr_auth() -> tuple[str, str]:
    """Return (username, password) for Checkr basic auth."""
    return (config.CHECKR_API_KEY, "")


def create_checkr_candidate(email: str, first_name: str, last_name: str) -> str | None:
    """Create a Checkr candidate. Returns the candidate ID or None."""
    resp = httpx.post(
        f"{CHECKR_BASE}/candidates",
        auth=_checkr_auth(),
        json={
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "no_middle_name": True,
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Checkr create candidate failed (%s): %s", resp.status_code, resp.text)
        return None
    return resp.json().get("id")


def create_checkr_invitation(candidate_id: str, work_state: str = "CA") -> str | None:
    """Create a Checkr invitation for a candidate. Returns the invitation ID or None."""
    resp = httpx.post(
        f"{CHECKR_BASE}/invitations",
        auth=_checkr_auth(),
        json={
            "candidate_id": candidate_id,
            "package": config.CHECKR_PACKAGE,
            "work_locations": [{"state": work_state, "country": "US"}],
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Checkr create invitation failed (%s): %s", resp.status_code, resp.text)
        return None
    return resp.json().get("id")


def get_checkr_report_status(invitation_id: str) -> str | None:
    """Check the status of a Checkr invitation. Returns status string or None."""
    resp = httpx.get(
        f"{CHECKR_BASE}/invitations/{invitation_id}",
        auth=_checkr_auth(),
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Checkr get invitation failed (%s): %s", resp.status_code, resp.text)
        return None

    data = resp.json()
    # Check report status if a report has been created
    report_id = data.get("report_id")
    if report_id:
        report_resp = httpx.get(
            f"{CHECKR_BASE}/reports/{report_id}",
            auth=_checkr_auth(),
            timeout=30,
        )
        if report_resp.status_code < 400:
            return report_resp.json().get("status")

    return data.get("status", "pending")


# ---------------------------------------------------------------------------
# Notion email backfill
# ---------------------------------------------------------------------------

def _backfill_email(page_id: str, page: dict, email: str) -> None:
    """Write email to Notion card if the Email property is currently empty."""
    props = page.get("properties", {})
    email_prop = props.get("Email", {})
    if email_prop.get("type") == "email" and email_prop.get("email"):
        return

    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {"Email": {"email": email}}},
        timeout=30,
    )
    if resp.status_code < 400:
        log.info("Backfilled email %s for page %s", email, page_id)
    else:
        log.warning("Failed to backfill email for %s: %s", page_id, resp.text[:200])


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
                log.warning("Rate limited by Slack, waiting %ds...", wait)
                time.sleep(wait)
            else:
                log.error("Slack API error: %s", e.response["error"])
                raise


# ---------------------------------------------------------------------------
# Missing-email notification
# ---------------------------------------------------------------------------

def _send_missing_email_alert(leaders_missing: list[tuple[str, str]]) -> None:
    """Email operations@kodely.io about leaders with no email on file.

    leaders_missing: list of (name, page_id) tuples.
    """
    if not leaders_missing:
        return

    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send missing-email alert for %d leader(s)", len(leaders_missing))
        return

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    lines = []
    for name, pid in leaders_missing:
        card_url = f"https://www.notion.so/{pid.replace('-', '')}"
        lines.append(f"  - {name}\n    Open their card to add it: {card_url}")
    names_list = "\n".join(lines)

    body = (
        f"Hi Servando,\n\n"
        f"The following leader(s) are missing an email address and cannot be "
        f"processed for background checks or onboarding:\n\n"
        f"{names_list}\n\n"
        f"Please open each card above and fill in the Email property, "
        f"or ensure the leader has filled out the Leader Confirmation Form.\n\n"
        f"Thanks,\n"
        f"Kodely Automation"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Action Needed: Missing email for {len(leaders_missing)} leader(s)"
    msg["From"] = config.EMAIL_FROM
    msg["To"] = "operations@kodely.io"
    msg.attach(MIMEText(body, "plain"))

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
            server.starttls(context=context)
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.EMAIL_FROM, ["operations@kodely.io"], msg.as_string())
        log.info("Sent missing-email alert to operations@kodely.io for %d leader(s)", len(leaders_missing))
    except Exception:
        log.exception("Failed to send missing-email alert")


# ---------------------------------------------------------------------------
# Sterling (under-18) notification
# ---------------------------------------------------------------------------

def _send_sterling_alert(leaders_sterling: list[tuple[str, str, str]]) -> None:
    """Email operations@kodely.io about under-18 leaders needing Sterling background check.

    leaders_sterling: list of (name, email, page_id) tuples.
    """
    if not leaders_sterling:
        return

    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send Sterling alert for %d leader(s)", len(leaders_sterling))
        return

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    lines = []
    for name, email, pid in leaders_sterling:
        card_url = f"https://www.notion.so/{pid.replace('-', '')}"
        lines.append(f"  - {name} ({email})\n    Notion card: {card_url}")
    names_list = "\n".join(lines)

    body = (
        f"Hi Servando,\n\n"
        f"The following leader(s) are under 18 and need a Sterling background "
        f"check instead of Checkr:\n\n"
        f"{names_list}\n\n"
        f"Please send a Sterling check for each leader listed above. "
        f"Once cleared, update their Notion card Compliance Status to \"Cleared\".\n\n"
        f"Thanks,\n"
        f"Kodely Automation"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Action Needed: Sterling background check for {len(leaders_sterling)} under-18 leader(s)"
    msg["From"] = config.EMAIL_FROM
    msg["To"] = "operations@kodely.io"
    msg.attach(MIMEText(body, "plain"))

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
            server.starttls(context=context)
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.EMAIL_FROM, ["operations@kodely.io"], msg.as_string())
        log.info("Sent Sterling alert to operations@kodely.io for %d under-18 leader(s)", len(leaders_sterling))
    except Exception:
        log.exception("Failed to send Sterling alert")


# ---------------------------------------------------------------------------
# Main pipeline: send new invitations + poll pending
# ---------------------------------------------------------------------------

def send_new_invitations(
    leaders: list[dict],
    state: dict,
    slack: SlackClient | None,
    form_emails: dict[str, str] | None = None,
    form_minors: set[str] | None = None,
    dry_run: bool = False,
) -> int:
    """Send Checkr invitations for leaders with Compliance = 'Not Sent'.

    First resolves emails from Notion or the Form Responses sheet.
    Then checks Checkr for an existing clear report within the last year.
    If already clear, sets Compliance = "Cleared" directly (no new check).
    Otherwise, creates a new Checkr invitation.

    Returns count of leaders processed (sent + auto-cleared).
    """
    if form_emails is None:
        form_emails = {}
    if form_minors is None:
        form_minors = set()

    processed = 0
    missing_email_names = []
    sterling_leaders = []
    for page in leaders:
        page_id = page.get("id", "")
        name = _get_leader_name(page)
        email = _resolve_email(page, form_emails)

        if not email:
            log.warning("No email for %s — cannot send Checkr invitation", name)
            clean = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', name).replace('\n', ' ').strip()
            if clean and not state.get(f"missing_email_notified_{page_id}"):
                missing_email_names.append((clean, page_id))
                state[f"missing_email_notified_{page_id}"] = datetime.now(timezone.utc).isoformat()
            continue

        # Check if leader is under 18 → needs Sterling instead of Checkr
        clean_name = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', name).replace('\n', ' ').strip()
        name_lower = clean_name.lower()
        is_minor = name_lower in form_minors
        if not is_minor and len(name_lower.split()) >= 2:
            parts = name_lower.split()
            for mn in form_minors:
                mp = mn.split()
                if len(mp) >= 2 and mp[0] == parts[0] and mp[-1] == parts[-1]:
                    is_minor = True
                    break

        if is_minor:
            if state.get(f"sterling_notified_{page_id}"):
                continue
            if dry_run:
                print(f"--- DRY RUN: UNDER-18 → STERLING NEEDED ---")
                print(f"  Leader: {clean_name} ({email})")
                print(f"  Under 18 — needs Sterling background check, not Checkr")
                print()
            else:
                sterling_leaders.append((clean_name, email, page_id))
                _patch_compliance_status(page_id, "Sterling Needed")
                log.info("Under-18 leader %s — flagged for Sterling (not Checkr)", clean_name)
                state[f"sterling_notified_{page_id}"] = datetime.now(timezone.utc).isoformat()
            processed += 1
            continue

        # Skip if already processed
        if state.get(f"sent_{page_id}") or state.get(f"cleared_{page_id}"):
            continue

        # Strip email from display name
        clean_name = re.sub(r'[\w.+-]+@[\w-]+\.[\w.]+', '', name).replace('\n', ' ').strip()
        parts = clean_name.split()
        first_name = parts[0] if parts else clean_name
        last_name = parts[-1] if len(parts) > 1 else ""

        # --- Check Checkr history: skip if clear within 1 year ---
        existing = check_existing_checkr(email, name=name)
        if existing == "clear":
            if dry_run:
                print(f"--- DRY RUN: CHECKR ALREADY CLEAR ---")
                print(f"  Leader: {clean_name} ({email})")
                print(f"  Existing clear report within 1 year — auto-clearing")
                print()
            else:
                _patch_compliance_status(page_id, "Cleared")
                log.info("Checkr already clear for %s — auto-set Compliance to Cleared", clean_name)

                # Also backfill email to Notion if it wasn't there
                _backfill_email(page_id, page, email)

                state[f"cleared_{page_id}"] = {
                    "reason": "existing_clear_report",
                    "email": email,
                    "at": datetime.now(timezone.utc).isoformat(),
                }

                if slack:
                    msg = (
                        f":white_check_mark: CHECKR ALREADY CLEAR\n\n"
                        f"*Leader:* {clean_name}\n"
                        f"*Email:* {email}\n\n"
                        f"Existing clear background check found (within 1 year) — auto-cleared."
                    )
                    try:
                        _post_to_slack(slack, msg)
                    except Exception:
                        log.exception("Failed to post Checkr auto-clear alert for %s", clean_name)

            processed += 1
            continue

        # --- No existing clear report — send new invitation ---
        if dry_run:
            work_state = _resolve_work_state(page)
            region = _get_property_value(page, "Region")
            print(f"--- DRY RUN: CHECKR INVITATION ---")
            print(f"  Leader: {clean_name} ({email})")
            print(f"  Region: {region} → State: {work_state}")
            print(f"  Package: {config.CHECKR_PACKAGE} ($37.99)")
            print(f"  No existing clear report — sending new check")
            print()
            processed += 1
            continue

        # Create candidate
        candidate_id = create_checkr_candidate(email, first_name, last_name)
        if not candidate_id:
            log.error("Failed to create Checkr candidate for %s", clean_name)
            continue

        # Create invitation with correct work state
        work_state = _resolve_work_state(page)
        log.info("Resolved work state for %s: %s (region: %s)", clean_name, work_state, _get_property_value(page, "Region"))
        invitation_id = create_checkr_invitation(candidate_id, work_state=work_state)
        if not invitation_id:
            log.error("Failed to create Checkr invitation for %s", clean_name)
            continue

        # Update Notion: Compliance Status = "Sent"
        if _patch_compliance_status(page_id, "Sent"):
            log.info("Checkr invitation sent for %s (inv: %s)", clean_name, invitation_id)

        # Backfill email to Notion if missing
        _backfill_email(page_id, page, email)

        # Track in state
        state[f"sent_{page_id}"] = {
            "invitation_id": invitation_id,
            "candidate_id": candidate_id,
            "name": clean_name,
            "email": email,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }

        # Slack alert
        if slack:
            msg = (
                f":shield: CHECKR BACKGROUND CHECK SENT\n\n"
                f"*Leader:* {clean_name}\n"
                f"*Email:* {email}\n"
                f"*Package:* {config.CHECKR_PACKAGE}\n\n"
                f"Background check invitation has been sent via Checkr."
            )
            try:
                _post_to_slack(slack, msg)
            except Exception:
                log.exception("Failed to post Checkr send alert for %s", clean_name)

        processed += 1

    # Send alert for leaders with missing emails
    if missing_email_names and not dry_run:
        _send_missing_email_alert(missing_email_names)

    # Send alert for under-18 leaders needing Sterling
    if sterling_leaders and not dry_run:
        _send_sterling_alert(sterling_leaders)

    return processed


def poll_pending(
    leaders: list[dict],
    state: dict,
    slack: SlackClient | None,
    dry_run: bool = False,
) -> int:
    """Poll Checkr for leaders with Compliance = 'Sent'.

    Returns count of leaders whose check cleared.
    """
    cleared = 0
    for page in leaders:
        page_id = page.get("id", "")
        name = _get_leader_name(page)
        sent_data = state.get(f"sent_{page_id}", {})
        invitation_id = sent_data.get("invitation_id")

        if not invitation_id:
            log.debug("No Checkr invitation ID in state for %s — skipping poll", name)
            continue

        if state.get(f"cleared_{page_id}"):
            continue

        if dry_run:
            print(f"--- DRY RUN: POLL CHECKR ---")
            print(f"  Leader: {name}")
            print(f"  Invitation: {invitation_id}")
            print()
            continue

        status = get_checkr_report_status(invitation_id)
        log.info("Checkr status for %s: %s", name, status)

        if status == "clear":
            if _patch_compliance_status(page_id, "Cleared"):
                log.info("Checkr CLEARED for %s — Compliance set to Cleared", name)

            state[f"cleared_{page_id}"] = datetime.now(timezone.utc).isoformat()

            if slack:
                msg = (
                    f":white_check_mark: CHECKR BACKGROUND CHECK CLEARED\n\n"
                    f"*Leader:* {name}\n\n"
                    f"Background check is clear — ready for onboarding setup."
                )
                try:
                    _post_to_slack(slack, msg)
                except Exception:
                    log.exception("Failed to post Checkr clear alert for %s", name)

            cleared += 1

        elif status in ("suspended", "dispute", "consider"):
            log.warning("Checkr status '%s' for %s — flagging", status, name)
            _patch_compliance_status(page_id, status.capitalize())

            if slack:
                msg = (
                    f":warning: CHECKR BACKGROUND CHECK — {status.upper()}\n\n"
                    f"*Leader:* {name}\n"
                    f"*Status:* {status}\n\n"
                    f"Manual review required."
                )
                try:
                    _post_to_slack(slack, msg)
                except Exception:
                    log.exception("Failed to post Checkr flag alert for %s", name)

    return cleared


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Checkr Background Check Sync")
    parser.add_argument("--dry-run", action="store_true", help="Print, don't act")
    args = parser.parse_args()

    if not config.CHECKR_API_KEY:
        log.error("CHECKR_API_KEY not set — exiting")
        return

    slack = None
    if not args.dry_run:
        slack = SlackClient(token=config.SLACK_BOT_TOKEN)

    state = load_state()

    # Load emails and minor status from Form Responses sheet
    log.info("Loading data from Form Responses sheet...")
    form_emails = load_form_emails()
    form_minors = load_form_minors()

    # Phase 1: Send invitations for "Not Sent" leaders
    not_sent = query_not_sent_leaders()
    log.info("Found %d leader(s) with Compliance = 'Not Sent'", len(not_sent))
    sent_count = send_new_invitations(not_sent, state, slack, form_emails=form_emails, form_minors=form_minors, dry_run=args.dry_run)

    # Phase 2: Poll pending invitations
    pending = query_pending_leaders()
    log.info("Found %d leader(s) with Compliance = 'Sent' (pending)", len(pending))
    cleared_count = poll_pending(pending, state, slack, dry_run=args.dry_run)

    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.CHECKR_STATE_PATH)

    log.info(
        "Done. %d invitation(s) sent, %d cleared%s.",
        sent_count, cleared_count,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
