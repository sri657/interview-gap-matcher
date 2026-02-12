#!/usr/bin/env python3
"""
Interview Gap Matcher

Queries Notion for candidates at "Team screening" / "Talent Screen" / "Teaching Demo"
stages, checks Google Sheets (Kodely Workshop Ops Hub) for workshops with true
leadership gaps (upcoming/current only), and posts matches to Slack #ops-matching.
"""

import argparse
import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone, date, timedelta
from urllib.parse import quote

import certifi
import gspread
import httpx
from google.oauth2.service_account import Credentials as ServiceCredentials
import google.auth.transport.requests
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from write_sheet import write_matches_to_sheet

# Fix macOS SSL certificate issue
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

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
# Dedup helpers
# ---------------------------------------------------------------------------

def load_notified() -> dict:
    try:
        with open(config.NOTIFIED_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_notified(data: dict) -> None:
    with open(config.NOTIFIED_PATH, "w") as f:
        json.dump(data, f, indent=2)


def notified_key(candidate_id: str, workshop_id: str) -> str:
    return f"{candidate_id}::{workshop_id}"


# ---------------------------------------------------------------------------
# Notion: fetch candidates ready for matching
# ---------------------------------------------------------------------------

def get_matchable_candidates() -> list[dict]:
    """Return candidates in any of the configured match-ready statuses."""
    status_filters = [
        {
            "property": config.NOTION_STATUS_PROPERTY,
            "select": {"equals": status},
        }
        for status in config.NOTION_STATUS_VALUES
    ]
    notion_filter = (
        {"or": status_filters} if len(status_filters) > 1 else status_filters[0]
    )

    results = []
    cursor = None

    while True:
        body: dict = {"filter": notion_filter, "page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = httpx.post(
            f"{NOTION_BASE}/databases/{config.NOTION_DATABASE_ID}/query",
            headers=NOTION_HEADERS,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        cutoff = datetime.now(timezone.utc) - timedelta(days=config.CANDIDATE_FRESHNESS_MONTHS * 30)

        for page in data.get("results", []):
            # Skip candidates whose Notion card is older than the freshness window
            created = page.get("created_time", "")
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    if created_dt < cutoff:
                        continue
                except ValueError:
                    pass

            props = page.get("properties", {})

            name = _extract_title(props.get(config.NOTION_NAME_PROPERTY, {}))

            status = ""
            status_prop = props.get(config.NOTION_STATUS_PROPERTY, {})
            if status_prop.get("select"):
                status = status_prop["select"]["name"]

            locations = []
            loc_prop = props.get(config.NOTION_LOCATION_PROPERTY, {})
            if loc_prop.get("type") == "multi_select":
                locations = [opt["name"] for opt in loc_prop.get("multi_select", [])]

            email = props.get("Email", {}).get("email", "") or ""

            # Format created date for display
            created_display = ""
            if created:
                try:
                    created_display = datetime.fromisoformat(
                        created.replace("Z", "+00:00")
                    ).strftime("%b %d, %Y")
                except ValueError:
                    pass

            results.append({
                "id": page["id"],
                "name": name,
                "status": status,
                "locations": locations,
                "email": email,
                "source": "notion",
                "source_date": created_display,
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    log.info(
        "Found %d candidate(s) in statuses: %s",
        len(results),
        ", ".join(config.NOTION_STATUS_VALUES),
    )
    return results


def _extract_title(prop: dict) -> str:
    if prop.get("type") == "title":
        return "".join(p.get("plain_text", "") for p in prop.get("title", []))
    return ""


# ---------------------------------------------------------------------------
# Google Sheets: fetch workshops with true gaps (upcoming/current only)
# ---------------------------------------------------------------------------

def _parse_date(date_str: str) -> date | None:
    """Parse date strings like 'November 3, 2025' or 'January 14, 2026'."""
    date_str = date_str.strip()
    if not date_str:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def _classify_leader_cell(bg: dict, strike: bool) -> str:
    """Classify a leader cell by its background color and strikethrough.

    Ops Hub color coding:
      Red    = backed out, needs restaffing          → 'red'       (GAP)
      Pink   = Scoot / 3rd-party agency              → 'scoot'     (GAP)
      Yellow = interviewing                          → 'yellow'    (not a gap)
      Green  = onboarding in progress                → 'green'     (not a gap)
      Purple = compliance                            → 'purple'    (not a gap)
      Grey   = cancelled                             → 'gray'      (excluded)
      Strikethrough = backed out                     → 'strikethrough' (GAP)
      White  = confirmed / normal                    → 'normal'    (not a gap)
    """
    # Google Sheets API omits color channels that are 0
    # e.g. pure red = {"red": 1}, pink = {"red": 1, "green": 0.6},
    #      purple = {"red": 0.6, "blue": 1}, white = {"red": 1, "green": 1, "blue": 1}
    r = bg.get("red", 0.0)
    g = bg.get("green", 0.0)
    b = bg.get("blue", 0.0)
    if not bg:
        r = g = b = 1.0

    # Strikethrough = backout
    if strike:
        return "strikethrough"
    # Gray bg (uniform 0.4-0.75) = cancelled / excluded
    if r < 0.75 and g < 0.75 and b < 0.75 and abs(r - g) < 0.05 and abs(g - b) < 0.05:
        return "gray"
    # Red bg: high red, low green, low blue — e.g. {red:1} = (1,0,0)
    if r > 0.7 and g < 0.4 and b < 0.4:
        return "red"
    # Orange bg (= matched): high red, mid green (~0.5-0.7), very low blue
    if r > 0.9 and 0.4 < g < 0.75 and b < 0.15:
        return "orange"
    # Pink bg (= Scoot): high red, mid green, low blue — e.g. {red:1, green:0.6} = (1,0.6,0)
    if r > 0.9 and 0.3 < g < 0.8 and b < 0.2:
        return "scoot"
    # Yellow bg (= interviewing): high red, high green, low blue
    if r > 0.8 and g > 0.8 and b < 0.4:
        return "yellow"
    # Green bg (= onboarding): green dominant
    if g > 0.6 and g > r and g > b:
        return "green"
    # Purple bg (= compliance): mid-high red, low green, high blue — e.g. {red:0.6, blue:1}
    if b > 0.5 and r > 0.3 and g < 0.5 and b > g:
        return "purple"
    return "normal"


def _is_pink(bg: dict) -> bool:
    """Legacy helper — check if a cell background is the pink tentative color."""
    r = bg.get("red", 0.0)
    g = bg.get("green", 0.0)
    b = bg.get("blue", 0.0)
    return r > 0.9 and 0.3 < g < 0.8 and b < 0.2


def _get_worksheet_by_gid(spreadsheet: gspread.Spreadsheet, gid: int) -> gspread.Worksheet:
    for ws in spreadsheet.worksheets():
        if ws.id == gid:
            return ws
    raise ValueError(f"No worksheet found with gid={gid}")


def _fetch_leader_formatting(creds: ServiceCredentials, total_rows: int) -> dict[int, list[str]]:
    """Fetch background colors and strikethrough for Leader 1/2/3 columns (T, U, V).

    Returns a dict mapping row_index (0-based data row) -> [l1_class, l2_class, l3_class]
    where each class is one of: 'pink', 'red', 'scoot', 'strikethrough', 'normal'.
    Only rows with at least one non-normal cell are included.
    """
    creds_copy = creds.with_scopes(["https://www.googleapis.com/auth/spreadsheets.readonly"])
    creds_copy.refresh(google.auth.transport.requests.Request())

    result = {}
    batch_size = 500
    for start in range(2, total_rows + 2, batch_size):
        end = min(start + batch_size - 1, total_rows + 1)
        range_str = f"Winter/Spring 26!T{start}:V{end}"
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{config.GOOGLE_SHEET_ID}"
        params = {
            "ranges": range_str,
            "fields": "sheets.data.rowData.values.effectiveFormat(backgroundColor,textFormat.strikethrough)",
        }
        headers = {"Authorization": f"Bearer {creds_copy.token}"}
        resp = httpx.get(url, headers=headers, params=params, timeout=60)
        data = resp.json()

        rows = data.get("sheets", [{}])[0].get("data", [{}])[0].get("rowData", [])
        for i, row in enumerate(rows):
            cells = row.get("values", [])
            classes = []
            for j in range(3):
                if j < len(cells):
                    fmt = cells[j].get("effectiveFormat", {})
                    bg = fmt.get("backgroundColor", {})
                    strike = fmt.get("textFormat", {}).get("strikethrough", False)
                    classes.append(_classify_leader_cell(bg, strike))
                else:
                    classes.append("normal")
            if any(c != "normal" for c in classes):
                result[start - 2 + i] = classes

    return result


def get_gap_workshops(gc: gspread.Client, creds: ServiceCredentials) -> list[dict]:
    """Return upcoming/current workshops that have true leadership gaps."""
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
        records.append({headers[j]: (row[i] if i < len(row) else "") for j, i in enumerate(valid_cols)})

    # Fetch leader column formatting for gap detection
    log.info("Fetching leader column formatting for %d rows...", len(records))
    fmt_map = _fetch_leader_formatting(creds, len(records))

    # Gap-indicating cell classes
    GAP_CLASSES = {"red", "scoot", "strikethrough"}

    today = date.today()
    gaps = []

    for idx, row in enumerate(records):
        # --- Skip cancelled programs ---
        setup = row.get("Setup", "").strip().upper()
        if "CANCEL" in setup:
            continue

        # --- Date filter: only upcoming or currently running ---
        end_date_str = row.get("End Date", "")
        end_dt = _parse_date(end_date_str)
        if end_dt and end_dt < today:
            continue

        region = row.get(config.SHEET_REGION_COL, "").strip()
        site = row.get(config.SHEET_SITE_COL, "").strip()
        lesson = row.get(config.SHEET_LESSON_COL, "").strip()

        if not region and not site:
            continue

        leader1 = row.get(config.SHEET_LEADER_1_COL, "").strip()
        leader2 = row.get(config.SHEET_LEADER_2_COL, "").strip()
        leader3 = row.get(config.SHEET_LEADER_3_COL, "").strip()

        # Classify each leader cell
        cell_classes = fmt_map.get(idx, ["normal", "normal", "normal"])
        leaders = [leader1, leader2, leader3]

        # Non-gap statuses: gray (cancelled), yellow (interviewing),
        # green (onboarding), purple (compliance), orange (matched), normal (confirmed)
        SKIP_CLASSES = {"gray", "yellow", "green", "purple", "orange", "normal"}

        # If ALL filled leaders are grey/cancelled, skip the entire row
        non_gray_leaders = [
            (leader, cls) for leader, cls in zip(leaders, cell_classes)
            if leader and cls != "gray"
        ]
        gray_count = sum(1 for leader, cls in zip(leaders, cell_classes) if leader and cls == "gray")
        if gray_count > 0 and not non_gray_leaders:
            continue  # all filled positions are cancelled — skip row

        # Determine gap type — only red, scoot (pink), and strikethrough are gaps
        all_empty = all(not leader or cls == "gray" for leader, cls in zip(leaders, cell_classes))
        gap_names: dict[str, list[str]] = {
            "backout": [],     # red, strikethrough
            "3rd_party": [],   # scoot (pink in Ops Hub)
        }
        has_gap_cell = False

        for leader, cls in zip(leaders, cell_classes):
            if not leader or cls in SKIP_CLASSES:
                continue
            if cls in ("red", "strikethrough"):
                gap_names["backout"].append(leader)
                has_gap_cell = True
            elif cls == "scoot":
                gap_names["3rd_party"].append(leader)
                has_gap_cell = True

        # A row is a gap if: all leaders empty/grey, OR any leader is red/scoot/strikethrough
        if not all_empty and not has_gap_cell:
            continue

        day = row.get(config.SHEET_DAY_COL, "").strip()
        start_time = row.get(config.SHEET_START_TIME_COL, "").strip()
        end_time = row.get(config.SHEET_END_TIME_COL, "").strip()
        time_str = f"{start_time}-{end_time}" if start_time and end_time else start_time or end_time
        start_date_str = row.get("Start Date", "").strip()

        # Determine the gap type label
        if all_empty:
            gap_type = "OPEN (no leaders)"
        elif gap_names["backout"]:
            gap_type = "BACKOUT"
        elif gap_names["3rd_party"]:
            gap_type = "3RD PARTY (Scoot)"
        else:
            gap_type = "OPEN (no leaders)"

        # Collect all flagged names
        flagged_names = gap_names["backout"] + gap_names["3rd_party"]

        district = row.get(config.SHEET_DISTRICT_COL, "").strip()
        zone = row.get(config.SHEET_ZONE_COL, "").strip()
        enrollment = row.get(config.SHEET_ENROLLMENT_COL, "").strip()
        level = row.get(config.SHEET_LEVEL_COL, "").strip()
        maps_query = f"{site} {region}".strip()
        maps_link = f"https://www.google.com/maps/search/{quote(maps_query)}"

        gaps.append({
            "region": region,
            "site": site,
            "lesson": lesson or "(unnamed)",
            "day": day,
            "time": time_str,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "gap_type": gap_type,
            "tentative_names": flagged_names,
            "workshop_key": f"{region}|{site}|{lesson}|{day}|{time_str}",
            "district": district,
            "zone": zone,
            "enrollment": enrollment,
            "level": level,
            "maps_link": maps_link,
        })

    log.info("Found %d upcoming workshop(s) with true gaps", len(gaps))
    return gaps


# ---------------------------------------------------------------------------
# Location normalization & matching
# ---------------------------------------------------------------------------

def normalize_location(loc: str) -> str:
    key = loc.strip().lower()
    return config.LOCATION_ALIASES.get(key, key)


WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}


def _parse_available_days(text: str) -> set[str]:
    """Extract weekday names from free-text availability like 'Monday, Tuesday, Friday'."""
    days = set()
    for word in text.lower().replace(",", " ").split():
        word = word.strip()
        if word in WEEKDAYS:
            days.add(word.capitalize())
        # Handle partial matches like "Tuesdays" -> "Tuesday"
        for wd in WEEKDAYS:
            if word.startswith(wd):
                days.add(wd.capitalize())
    return days


def get_form_candidates(gc: "gspread.Client") -> list[dict]:
    """Return candidates from the Leader Confirmation Form Responses sheet.

    Filters out Inactive rows and rows older than CANDIDATE_FRESHNESS_MONTHS.
    """
    spreadsheet = gc.open_by_key(config.FORM_SHEET_ID)
    for ws in spreadsheet.worksheets():
        if ws.id == config.FORM_SHEET_GID:
            sheet = ws
            break
    else:
        log.warning("Form Responses sheet (GID %s) not found", config.FORM_SHEET_GID)
        return []

    all_rows = sheet.get_all_values()
    if not all_rows:
        return []

    headers = all_rows[0]
    col_map = {h: i for i, h in enumerate(headers)}

    def _col(key: str) -> int:
        # Try exact match first, then prefix match for long column names
        if key in col_map:
            return col_map[key]
        for h, i in col_map.items():
            if h.startswith(key[:40]):
                return i
        return -1

    name_idx = _col(config.FORM_NAME_COL)
    email_idx = _col(config.FORM_EMAIL_COL)
    days_idx = _col(config.FORM_DAYS_COL)
    loc_idx = _col(config.FORM_LOCATION_COL)
    date_idx = _col(config.FORM_DATE_COL)
    status_idx = _col(config.FORM_STATUS_COL)
    returning_idx = _col(config.FORM_RETURNING_COL)

    cutoff = date.today() - timedelta(days=config.CANDIDATE_FRESHNESS_MONTHS * 30)
    results = []

    for row in all_rows[1:]:
        def _get(idx: int) -> str:
            return row[idx].strip() if 0 <= idx < len(row) else ""

        # Skip inactive
        if _get(status_idx).lower() == "inactive":
            continue

        # Parse and check form submission date
        date_str = _get(date_idx)
        form_date = None
        if date_str:
            # Format: "2/11/2026 11:23:21"
            try:
                form_date = datetime.strptime(date_str.split()[0], "%m/%d/%Y").date()
            except ValueError:
                pass
        if form_date and form_date < cutoff:
            continue

        name = _get(name_idx)
        email = _get(email_idx)
        if not name:
            continue

        # Parse locations — may be comma-separated free text
        raw_loc = _get(loc_idx)
        locations = [loc.strip() for loc in raw_loc.split(",") if loc.strip()] if raw_loc else []

        # Parse available days
        available_days = _parse_available_days(_get(days_idx))

        returning = _get(returning_idx)
        status = "Returning Leader" if returning.lower() == "yes" else "Form Applicant"

        # Format form date for display
        form_date_display = ""
        if form_date:
            form_date_display = form_date.strftime("%b %d, %Y")

        results.append({
            "id": f"form::{email or name}",
            "name": name,
            "status": status,
            "locations": locations,
            "email": email,
            "available_days": available_days,
            "source": "form",
            "source_date": form_date_display,
        })

    log.info("Found %d form candidate(s) within %d-month window", len(results), config.CANDIDATE_FRESHNESS_MONTHS)
    return results


def find_matches(
    candidates: list[dict], workshops: list[dict]
) -> list[tuple[dict, list[dict]]]:
    region_index: dict[str, list[dict]] = {}
    for ws in workshops:
        key = normalize_location(ws["region"])
        if key:
            region_index.setdefault(key, []).append(ws)

    matches = []
    for candidate in candidates:
        matched_ws = []
        for loc in candidate["locations"]:
            key = normalize_location(loc)
            matched_ws.extend(region_index.get(key, []))

        # For form candidates, filter workshops to matching days when possible
        cand_days = candidate.get("available_days", set())
        if cand_days and candidate.get("source") == "form":
            day_filtered = [
                ws for ws in matched_ws
                if ws.get("day", "").strip().capitalize() in cand_days
            ]
            # Only apply day filter if it still leaves some matches
            if day_filtered:
                matched_ws = day_filtered

        seen = set()
        unique = []
        for ws in matched_ws:
            if ws["workshop_key"] not in seen:
                seen.add(ws["workshop_key"])
                unique.append(ws)
        if unique:
            matches.append((candidate, unique))

    log.info("Found %d candidate(s) with matching gap workshops", len(matches))
    return matches


# ---------------------------------------------------------------------------
# Slack notification
# ---------------------------------------------------------------------------

def build_slack_message(candidate: dict, workshops: list[dict]) -> str:
    locations_str = ", ".join(candidate["locations"]) if candidate["locations"] else "(none)"
    email = candidate["email"] or "(no email on file)"

    workshop_lines = []
    for ws in workshops:
        tentative = ""
        if ws["tentative_names"]:
            tentative = f" (tentative: {', '.join(ws['tentative_names'])})"
        dates = ""
        if ws["start_date"] or ws["end_date"]:
            dates = f"  |  {ws['start_date']} \u2013 {ws['end_date']}"
        meta_parts = []
        if ws.get("district"):
            meta_parts.append(f"District: {ws['district']}")
        if ws.get("enrollment"):
            meta_parts.append(f"Enrollment: {ws['enrollment']}")
        if ws.get("level"):
            meta_parts.append(f"Level: {ws['level']}")
        bullet = " \u2022 "
        meta_str = f"  |  {bullet.join(meta_parts)}" if meta_parts else ""
        maps_str = "\n    \U0001f4cd " + ws['maps_link']
        workshop_lines.append(
            f"  \u2022 {ws['lesson']} @ {ws['site']} \u2014 {ws['day']}s {ws['time']}{dates}"
            f"  [{ws['gap_type']}]{tentative}{meta_str}{maps_str}"
        )
    ws_block = "\n".join(workshop_lines)

    # Build a ready-to-send email template
    first_name = candidate["name"].split()[0] if candidate["name"] else "there"
    workshop_list_for_email = "\n".join(
        f"  - {ws['lesson']} at {ws['site']} — {ws['day']}s {ws['time']} ({ws['start_date']} to {ws['end_date']})"
        for ws in workshops
    )

    email_template = (
        f"Subject: Workshop Opportunity at Kodely\n\n"
        f"Hi {first_name},\n\n"
        f"We have an opening for a workshop leader at {workshops[0]['site']} and think "
        f"you'd be a great fit! "
        f"Here are the available workshops in your area:\n\n"
        f"{workshop_list_for_email}\n\n"
        f"Would any of these work for your schedule? Let us know and we can get "
        f"the offer process started.\n\n"
        f"Best,\nKodely Ops Team"
    )

    return (
        f"*Gap Match Found*\n\n"
        f"*Candidate:* {candidate['name']}\n"
        f"*Email:* {email}\n"
        f"*Pipeline Status:* {candidate['status']}\n"
        f"*Location(s):* {locations_str}\n\n"
        f"*Open Workshop Gap(s):*\n{ws_block}\n\n"
        f"\u27a1\ufe0f *Draft Email:*\n```\n{email_template}\n```"
    )


def post_to_slack(slack: SlackClient, message: str, retries: int = 5) -> None:
    for attempt in range(retries):
        try:
            slack.chat_postMessage(channel=config.SLACK_CHANNEL, text=message)
            return
        except SlackApiError as e:
            if e.response["error"] == "ratelimited" and attempt < retries - 1:
                wait = int(e.response.headers.get("Retry-After", 10))
                log.warning("Rate limited by Slack, waiting %ds (attempt %d/%d)...", wait, attempt + 1, retries)
                time.sleep(wait)
            else:
                log.error("Slack API error: %s", e.response["error"])
                raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Interview Gap Matcher")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query APIs and print matches without posting to Slack",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Only match workshops in this region (e.g. 'SF', 'LA', 'Manhattan')",
    )
    args = parser.parse_args()

    # --- Initialise clients ---
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceCredentials.from_service_account_file(config.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)

    slack = SlackClient(token=config.SLACK_BOT_TOKEN)

    # --- Fetch data ---
    candidates = get_matchable_candidates()
    form_candidates = get_form_candidates(gc)
    candidates.extend(form_candidates)
    log.info("Total candidates (Notion + Form): %d", len(candidates))
    workshops = get_gap_workshops(gc, creds)

    if args.region:
        region_filter = normalize_location(args.region)
        workshops = [ws for ws in workshops if normalize_location(ws["region"]) == region_filter]
        log.info("Filtered to %d workshop(s) in region '%s'", len(workshops), args.region)

    matches = find_matches(candidates, workshops)

    if not matches:
        log.info("No new matches found. Exiting.")
        return

    # --- Dedup & notify ---
    notified = load_notified()
    new_notifications = 0

    for candidate, matched_ws in matches:
        unseen = [
            ws for ws in matched_ws
            if notified_key(candidate["id"], ws["workshop_key"]) not in notified
        ]
        if not unseen:
            continue

        message = build_slack_message(candidate, unseen)

        if args.dry_run:
            print("--- DRY RUN ---")
            print(message)
            print()
        else:
            post_to_slack(slack, message)
            log.info("Posted match for %s", candidate["name"])
            time.sleep(1.2)  # stay under Slack's ~1 msg/sec rate limit

        now = datetime.now(timezone.utc).isoformat()
        for ws in unseen:
            notified[notified_key(candidate["id"], ws["workshop_key"])] = now
        new_notifications += 1

        # Save after each post so progress isn't lost on crash
        if not args.dry_run and new_notifications % 5 == 0:
            save_notified(notified)

    save_notified(notified)

    # --- Update Gap Matches sheet tab ---
    if not args.dry_run:
        try:
            write_matches_to_sheet(gc, matches)
        except Exception:
            log.exception("Failed to write matches to Google Sheet tab")

    log.info(
        "Done. %d new notification(s) sent%s.",
        new_notifications,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
