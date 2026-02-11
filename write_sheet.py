"""
Write gap matches to the "Gap Matches" tab in the Ops Hub Google Sheet.

Clears and rewrites all rows each run so the tab always reflects the current state.
"""

import logging
from datetime import date

import gspread

import config

log = logging.getLogger(__name__)

HEADERS = [
    "Date",
    "Region",
    "Site",
    "Lesson",
    "Day",
    "Time",
    "Start Date",
    "End Date",
    "Gap Type",
    "Candidate",
    "Email",
    "Status",
]


def _get_or_create_tab(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    """Return the Gap Matches worksheet, creating it if it doesn't exist."""
    tab_name = config.SHEET_MATCHES_TAB_NAME
    try:
        return spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        log.info("Creating new tab '%s'", tab_name)
        return spreadsheet.add_worksheet(title=tab_name, rows=500, cols=len(HEADERS))


def write_matches_to_sheet(
    gc: gspread.Client,
    matches: list[tuple[dict, list[dict]]],
) -> None:
    """Clear and rewrite the Gap Matches tab with all current matches.

    Args:
        gc: An authorized gspread client (needs Editor access on the sheet).
        matches: List of (candidate, [workshops]) tuples from find_matches().
    """
    spreadsheet = gc.open_by_key(config.GOOGLE_SHEET_ID)
    ws = _get_or_create_tab(spreadsheet)

    today_str = date.today().isoformat()

    rows = [HEADERS]
    for candidate, workshops in matches:
        for ws_item in workshops:
            rows.append([
                today_str,
                ws_item["region"],
                ws_item["site"],
                ws_item["lesson"],
                ws_item["day"],
                ws_item["time"],
                ws_item["start_date"],
                ws_item["end_date"],
                ws_item["gap_type"],
                candidate["name"],
                candidate["email"] or "",
                candidate["status"],
            ])

    ws.clear()
    if rows:
        ws.update(rows, value_input_option="USER_ENTERED")

    log.info(
        "Wrote %d match row(s) to '%s' tab",
        len(rows) - 1,
        config.SHEET_MATCHES_TAB_NAME,
    )
