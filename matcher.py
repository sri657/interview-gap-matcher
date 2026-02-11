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
from datetime import datetime, timezone, date

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

        for page in data.get("results", []):
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

            results.append({
                "id": page["id"],
                "name": name,
                "status": status,
                "locations": locations,
                "email": email,
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


def _is_pink(bg: dict) -> bool:
    """Check if a cell background is the pink/purple tentative color."""
    r = bg.get("red", 1.0)
    g = bg.get("green", 1.0)
    b = bg.get("blue", 1.0)
    return r > 0.9 and g < 0.7 and b > 0.9


def _get_worksheet_by_gid(spreadsheet: gspread.Spreadsheet, gid: int) -> gspread.Worksheet:
    for ws in spreadsheet.worksheets():
        if ws.id == gid:
            return ws
    raise ValueError(f"No worksheet found with gid={gid}")


def _fetch_leader_formatting(creds: ServiceCredentials, total_rows: int) -> dict[int, list[bool]]:
    """Fetch background colors for Leader 1/2/3 columns (T, U, V) and return
    a dict mapping row_index (0-based data row) -> [l1_pink, l2_pink, l3_pink]."""
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
            "fields": "sheets.data.rowData.values.effectiveFormat.backgroundColor",
        }
        headers = {"Authorization": f"Bearer {creds_copy.token}"}
        resp = httpx.get(url, headers=headers, params=params, timeout=60)
        data = resp.json()

        rows = data.get("sheets", [{}])[0].get("data", [{}])[0].get("rowData", [])
        for i, row in enumerate(rows):
            cells = row.get("values", [])
            pinks = []
            for j in range(3):
                if j < len(cells):
                    bg = cells[j].get("effectiveFormat", {}).get("backgroundColor", {})
                    pinks.append(_is_pink(bg))
                else:
                    pinks.append(False)
            if any(pinks):
                result[start - 2 + i] = pinks  # 0-based data row index

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

    # Fetch leader column formatting for tentative detection
    log.info("Fetching leader column formatting for %d rows...", len(records))
    pink_map = _fetch_leader_formatting(creds, len(records))

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

        # Check pink/tentative status for this row
        pinks = pink_map.get(idx, [False, False, False])

        # Determine gap type
        all_empty = not leader1 and not leader2 and not leader3
        has_tentative = False
        tentative_names = []

        if pinks[0] and leader1:
            has_tentative = True
            tentative_names.append(leader1)
        if pinks[1] and leader2:
            has_tentative = True
            tentative_names.append(leader2)
        if pinks[2] and leader3:
            has_tentative = True
            tentative_names.append(leader3)

        if not all_empty and not has_tentative:
            continue

        day = row.get(config.SHEET_DAY_COL, "").strip()
        start_time = row.get(config.SHEET_START_TIME_COL, "").strip()
        end_time = row.get(config.SHEET_END_TIME_COL, "").strip()
        time_str = f"{start_time}-{end_time}" if start_time and end_time else start_time or end_time
        start_date_str = row.get("Start Date", "").strip()

        gap_type = "OPEN (no leaders)" if all_empty else "TENTATIVE (interview only)"

        gaps.append({
            "region": region,
            "site": site,
            "lesson": lesson or "(unnamed)",
            "day": day,
            "time": time_str,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "gap_type": gap_type,
            "tentative_names": tentative_names,
            "workshop_key": f"{region}|{site}|{lesson}|{day}|{time_str}",
        })

    log.info("Found %d upcoming workshop(s) with true gaps", len(gaps))
    return gaps


# ---------------------------------------------------------------------------
# Location normalization & matching
# ---------------------------------------------------------------------------

def normalize_location(loc: str) -> str:
    key = loc.strip().lower()
    return config.LOCATION_ALIASES.get(key, key)


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
        workshop_lines.append(
            f"  \u2022 {ws['lesson']} @ {ws['site']} \u2014 {ws['day']}s {ws['time']}{dates}"
            f"  [{ws['gap_type']}]{tentative}"
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
        f"We have an opening for a workshop leader and think you'd be a great fit! "
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


def post_to_slack(slack: SlackClient, message: str) -> None:
    try:
        slack.chat_postMessage(channel=config.SLACK_CHANNEL, text=message)
    except SlackApiError as e:
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

        now = datetime.now(timezone.utc).isoformat()
        for ws in unseen:
            notified[notified_key(candidate["id"], ws["workshop_key"])] = now
        new_notifications += 1

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
