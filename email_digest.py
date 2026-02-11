#!/usr/bin/env python3
"""
Daily email digest of gap matches.

Queries Notion + Google Sheets for current gaps and matched candidates,
builds an HTML email, and sends it to talent@kodely.io.

Usage:
    python email_digest.py              # send the digest email
    python email_digest.py --dry-run    # preview HTML to stdout without sending
"""

import argparse
import logging
import os
import smtplib
import ssl
from collections import defaultdict
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import certifi
import gspread
from google.oauth2.service_account import Credentials as ServiceCredentials

import config
from matcher import (
    find_matches,
    get_gap_workshops,
    get_matchable_candidates,
    normalize_location,
)

os.environ.setdefault("SSL_CERT_FILE", certifi.where())
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

def _build_html(
    matches: list[tuple[dict, list[dict]]],
    workshops: list[dict],
) -> str:
    """Build an HTML digest email body."""
    today_str = date.today().strftime("%B %d, %Y")
    total_gaps = len(workshops)
    total_candidates = len(matches)

    # Group matches by normalized region
    by_region: dict[str, list[tuple[dict, list[dict]]]] = defaultdict(list)
    for candidate, ws_list in matches:
        regions_seen = set()
        for ws in ws_list:
            region = normalize_location(ws["region"]) or ws["region"]
            if region not in regions_seen:
                regions_seen.add(region)
            by_region[region].append((candidate, [w for w in ws_list if normalize_location(w["region"]) == region]))
        # deduplicate per region
    # Re-deduplicate: a candidate may appear multiple times per region
    deduped: dict[str, list[tuple[dict, list[dict]]]] = {}
    for region, items in by_region.items():
        seen_ids = set()
        unique = []
        for cand, ws_list in items:
            if cand["id"] not in seen_ids:
                seen_ids.add(cand["id"])
                unique.append((cand, ws_list))
        deduped[region] = unique
    by_region = deduped

    region_blocks = []
    for region in sorted(by_region):
        cand_rows = ""
        for candidate, ws_list in by_region[region]:
            ws_lines = ""
            for ws in ws_list:
                tentative = ""
                if ws.get("tentative_names"):
                    tentative = f' <span style="color:#999">(tentative: {", ".join(ws["tentative_names"])})</span>'
                meta_parts = []
                if ws.get("district"):
                    meta_parts.append(f"District: {ws['district']}")
                if ws.get("enrollment"):
                    meta_parts.append(f"Enrollment: {ws['enrollment']}")
                if ws.get("level"):
                    meta_parts.append(f"Level: {ws['level']}")
                meta_html = ""
                if meta_parts:
                    sep = " &bull; "
                    meta_html = f'<br><span style="font-size:11px;color:#888">{sep.join(meta_parts)}</span>'
                maps_html = ""
                if ws.get("maps_link"):
                    maps_html = f' <a href="{ws["maps_link"]}" style="font-size:11px">&#x1f4cd; Map</a>'
                ws_lines += (
                    f"<li>{ws['lesson']} @ {ws['site']} &mdash; "
                    f"{ws['day']}s {ws['time']} "
                    f"({ws['start_date']} &ndash; {ws['end_date']}) "
                    f"<b>[{ws['gap_type']}]</b>{tentative}{maps_html}{meta_html}</li>\n"
                )

            first_name = candidate["name"].split()[0] if candidate["name"] else "there"
            site_name = ws_list[0]["site"] if ws_list else "a school"
            draft = (
                f"Hi {first_name}, we have an opening for a workshop leader at {site_name} "
                f"and think you'd be a great fit! Would any of these work for your schedule?"
            )

            cand_rows += f"""
            <tr>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top">{candidate['name']}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top">{candidate['email'] or '(none)'}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top">{candidate['status']}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top"><ul style="margin:0;padding-left:16px">{ws_lines}</ul></td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;font-size:12px;color:#666"><i>{draft}</i></td>
            </tr>"""

        region_blocks.append(f"""
        <h2 style="color:#333;border-bottom:2px solid #4a90d9;padding-bottom:4px">{region.upper()}</h2>
        <table style="border-collapse:collapse;width:100%;margin-bottom:24px">
          <tr style="background:#4a90d9;color:#fff">
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Candidate</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Email</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Status</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Gap Workshop(s)</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Draft Outreach</th>
          </tr>
          {cand_rows}
        </table>""")

    no_match_note = ""
    if not matches:
        no_match_note = '<p style="color:#999;font-style:italic">No candidate matches found for current gaps.</p>'

    regions_html = "\n".join(region_blocks)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;padding:16px">
  <h1 style="color:#4a90d9">Kodely Gap Match Digest</h1>
  <p style="color:#666">{today_str}</p>

  <div style="background:#f5f7fa;padding:12px 16px;border-radius:6px;margin-bottom:24px">
    <b>{total_gaps}</b> workshop gap(s) &nbsp;|&nbsp; <b>{total_candidates}</b> matched candidate(s)
  </div>

  {no_match_note}
  {regions_html}

  <hr style="border:none;border-top:1px solid #ddd;margin-top:32px">
  <p style="font-size:12px;color:#999">
    Automated digest from the Interview Gap Matcher.
    Matches are based on candidate location and pipeline stage.
  </p>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def send_email(html: str, subject: str) -> None:
    """Send the digest email via SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_FROM
    msg["To"] = config.EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    context = ssl.create_default_context(cafile=certifi.where())
    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
        server.starttls(context=context)
        server.login(config.SMTP_USER, config.SMTP_PASSWORD)
        server.sendmail(config.EMAIL_FROM, [config.EMAIL_TO], msg.as_string())

    log.info("Digest email sent to %s", config.EMAIL_TO)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Gap Match Email Digest")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the HTML email to stdout without sending",
    )
    args = parser.parse_args()

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = ServiceCredentials.from_service_account_file(config.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)

    candidates = get_matchable_candidates()
    workshops = get_gap_workshops(gc, creds)
    matches = find_matches(candidates, workshops)

    today_str = date.today().strftime("%Y-%m-%d")
    subject = f"Kodely Gap Match Digest — {today_str}"
    html = _build_html(matches, workshops)

    if args.dry_run:
        print(html)
        log.info("Dry run complete — %d matches across %d gaps", len(matches), len(workshops))
    else:
        send_email(html, subject)


if __name__ == "__main__":
    main()
