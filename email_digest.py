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
    _parse_date,
    find_matches,
    get_form_candidates,
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

def _urgency_key(ws: dict) -> tuple:
    """Sort key: OPEN > BACKOUT > 3RD PARTY, then by earliest start date."""
    gt = ws["gap_type"]
    if "OPEN" in gt:
        type_rank = 0
    elif "BACKOUT" in gt:
        type_rank = 1
    elif "3RD PARTY" in gt:
        type_rank = 2
    else:
        type_rank = 3
    start = _parse_date(ws["start_date"]) or date.max
    return (type_rank, start)


def _build_html(
    matches: list[tuple[dict, list[dict]]],
    workshops: list[dict],
) -> str:
    """Build a gap-centric HTML digest email body."""
    today_str = date.today().strftime("%B %d, %Y")
    total_gaps = len(workshops)
    total_candidates = len(matches)

    # --- 1. Invert data: build gap -> candidates reverse index ---
    gap_candidates: dict[str, list[dict]] = defaultdict(list)
    gap_workshop: dict[str, dict] = {}
    for candidate, ws_list in matches:
        for ws in ws_list:
            key = ws["workshop_key"]
            gap_workshop[key] = ws
            if candidate["id"] not in {c["id"] for c in gap_candidates[key]}:
                gap_candidates[key].append(candidate)

    # --- 2. Group by region, sort regions by gap count desc ---
    region_gaps: dict[str, list[dict]] = defaultdict(list)
    for ws in gap_workshop.values():
        region = normalize_location(ws["region"]) or ws["region"]
        region_gaps[region].append(ws)
    sorted_regions = sorted(region_gaps, key=lambda r: len(region_gaps[r]), reverse=True)

    # --- Region heat-map summary line ---
    heat_parts = [f"{r.upper()}: {len(region_gaps[r])} gaps" for r in sorted_regions]
    heat_line = " &nbsp;|&nbsp; ".join(heat_parts) if heat_parts else ""

    # --- 3 & 4. Build each region's gap-centric table ---
    td = 'style="padding:8px;border:1px solid #ddd;vertical-align:top"'
    th = 'style="padding:8px;border:1px solid #ddd;text-align:left"'

    region_blocks = []
    for region in sorted_regions:
        ws_list = sorted(region_gaps[region], key=_urgency_key)
        gap_rows = ""
        for ws in ws_list:
            # Site cell with maps link
            maps_html = ""
            if ws.get("maps_link"):
                maps_html = f' <a href="{ws["maps_link"]}" style="font-size:11px">&#x1f4cd; Map</a>'
            site_cell = f"{ws['site']}{maps_html}"

            # Workshop details
            detail_cell = (
                f"{ws['lesson']}<br>"
                f"{ws['day']}s {ws['time']}<br>"
                f"{ws['start_date']} &ndash; {ws['end_date']}"
            )

            # School info
            meta_parts = []
            if ws.get("district"):
                meta_parts.append(f"District: {ws['district']}")
            if ws.get("enrollment"):
                meta_parts.append(f"Enrollment: {ws['enrollment']}")
            if ws.get("level"):
                meta_parts.append(f"Level: {ws['level']}")
            school_cell = "<br>".join(meta_parts) if meta_parts else "&mdash;"

            # Gap type with tentative names
            if "OPEN" in ws["gap_type"]:
                gap_color = "#c0392b"
            elif "BACKOUT" in ws["gap_type"]:
                gap_color = "#b71c1c"
            elif "3RD PARTY" in ws["gap_type"]:
                gap_color = "#00838f"
            else:
                gap_color = "#e67e22"
            gap_label = f'<span style="color:{gap_color};font-weight:bold">{ws["gap_type"]}</span>'
            if ws.get("tentative_names"):
                gap_label += f'<br><span style="font-size:11px;color:#999">{", ".join(ws["tentative_names"])}</span>'

            # Candidates for this gap
            candidates_for_gap = gap_candidates.get(ws["workshop_key"], [])
            if candidates_for_gap:
                cand_lines = []
                for c in candidates_for_gap:
                    email_str = f' &lt;{c["email"]}&gt;' if c.get("email") else ""
                    source = c.get("source", "notion")
                    if source == "form":
                        badge = '<span style="font-size:10px;background:#e8f5e9;color:#2e7d32;padding:1px 4px;border-radius:3px">FORM</span> '
                    else:
                        badge = '<span style="font-size:10px;background:#e3f2fd;color:#1565c0;padding:1px 4px;border-radius:3px">NOTION</span> '
                    days_str = ""
                    cand_days = c.get("available_days")
                    if cand_days:
                        days_str = f' <span style="font-size:10px;color:#666">({", ".join(sorted(cand_days))})</span>'
                    date_str = ""
                    if c.get("source_date"):
                        date_str = f' <span style="font-size:10px;color:#999">{c["source_date"]}</span>'
                    cand_lines.append(
                        f'{badge}{c["name"]}{email_str}'
                        f' <span style="font-size:11px;color:#888">[{c["status"]}]</span>{days_str}{date_str}'
                    )
                cand_cell = "<br>".join(cand_lines)
            else:
                cand_cell = '<span style="color:#999">No matches</span>'

            gap_rows += f"""
            <tr>
              <td {td}>{site_cell}</td>
              <td {td}>{detail_cell}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;font-size:12px">{school_cell}</td>
              <td {td}>{gap_label}</td>
              <td {td}>{cand_cell}</td>
            </tr>"""

        region_blocks.append(f"""
        <h2 style="color:#333;border-bottom:2px solid #4a90d9;padding-bottom:4px">
          {region.upper()} <span style="font-size:14px;color:#888">({len(ws_list)} gap{"s" if len(ws_list) != 1 else ""})</span>
        </h2>
        <table style="border-collapse:collapse;width:100%;margin-bottom:24px">
          <tr style="background:#4a90d9;color:#fff">
            <th {th}>Site</th>
            <th {th}>Workshop Details</th>
            <th {th}>School Info</th>
            <th {th}>Gap Type</th>
            <th {th}>Available Candidates</th>
          </tr>
          {gap_rows}
        </table>""")

    no_match_note = ""
    if not matches:
        no_match_note = '<p style="color:#999;font-style:italic">No candidate matches found for current gaps.</p>'

    regions_html = "\n".join(region_blocks)

    # --- 5. Full candidate roster by region ---
    # Collect all unique candidates per region
    region_all_candidates: dict[str, list[dict]] = defaultdict(list)
    seen_cand_region: set[tuple[str, str]] = set()
    for candidate, ws_list in matches:
        for ws in ws_list:
            region = normalize_location(ws["region"]) or ws["region"]
            key = (candidate["id"], region)
            if key not in seen_cand_region:
                seen_cand_region.add(key)
                region_all_candidates[region].append(candidate)

    roster_blocks = []
    for region in sorted_regions:
        cands = region_all_candidates.get(region, [])
        if not cands:
            continue
        # Separate FORM vs NOTION, sort alphabetically
        form_cands = sorted([c for c in cands if c.get("source") == "form"], key=lambda c: c["name"].lower())
        notion_cands = sorted([c for c in cands if c.get("source") != "form"], key=lambda c: c["name"].lower())

        roster_rows = ""
        for c in form_cands + notion_cands:
            source = c.get("source", "notion")
            if source == "form":
                badge = '<span style="background:#e8f5e9;color:#2e7d32;padding:2px 6px;border-radius:3px;font-size:11px;font-weight:bold">FORM</span>'
            else:
                badge = '<span style="background:#e3f2fd;color:#1565c0;padding:2px 6px;border-radius:3px;font-size:11px;font-weight:bold">NOTION</span>'
            days_str = ", ".join(sorted(c.get("available_days", []))) or "&mdash;"
            email_display = c.get("email") or "(none)"
            source_date = c.get("source_date", "") or "&mdash;"
            roster_rows += f"""
            <tr>
              <td {td}>{badge}</td>
              <td {td}><b>{c['name']}</b></td>
              <td {td}>{email_display}</td>
              <td {td}>{c['status']}</td>
              <td {td} style="padding:8px;border:1px solid #ddd;vertical-align:top;font-size:12px">{days_str}</td>
              <td {td} style="padding:8px;border:1px solid #ddd;vertical-align:top;font-size:12px">{source_date}</td>
            </tr>"""

        roster_blocks.append(f"""
        <h3 style="margin-top:16px">{region.upper()} &mdash; {len(cands)} candidate{"s" if len(cands) != 1 else ""}
          <span style="font-size:12px;color:#888">({len(form_cands)} form, {len(notion_cands)} notion)</span>
        </h3>
        <table style="border-collapse:collapse;width:100%;margin-bottom:12px">
          <tr style="background:#607d8b;color:#fff">
            <th {th}>Source</th>
            <th {th}>Name</th>
            <th {th}>Email</th>
            <th {th}>Status</th>
            <th {th}>Available Days</th>
            <th {th}>Date</th>
          </tr>
          {roster_rows}
        </table>""")

    roster_html = "\n".join(roster_blocks)

    # --- 6. Action checklist per gap: templates for Campaign, BCC email, Form email ---
    matched_keys = set(gap_workshop.keys())
    all_gap_workshops = list(gap_workshop.values()) + [
        ws for ws in workshops if ws["workshop_key"] not in matched_keys
    ]
    # Group ALL gaps by region for the checklist
    checklist_by_region: dict[str, list[dict]] = defaultdict(list)
    for ws in all_gap_workshops:
        region = normalize_location(ws["region"]) or ws["region"]
        if ws["workshop_key"] not in {w["workshop_key"] for w in checklist_by_region[region]}:
            checklist_by_region[region].append(ws)

    season = "Winter/Spring 2026"
    checklist_blocks = []
    gap_num = 0
    for region in sorted(checklist_by_region, key=lambda r: len(checklist_by_region[r]), reverse=True):
        region_upper = region.upper()
        gap_cards = ""
        for ws in sorted(checklist_by_region[region], key=_urgency_key):
            gap_num += 1
            has_candidates = bool(gap_candidates.get(ws["workshop_key"]))
            if "OPEN" in ws["gap_type"]:
                gap_color = "#c0392b"
            elif "BACKOUT" in ws["gap_type"]:
                gap_color = "#b71c1c"
            elif "3RD PARTY" in ws["gap_type"]:
                gap_color = "#00838f"
            else:
                gap_color = "#e67e22"
            level_str = f" (Grades {ws['level']})" if ws.get("level") else ""
            district_str = f", {ws['district']}" if ws.get("district") else ""

            # ---- Roles block for this gap ----
            role_block = (
                f"{ws['site']} ({ws['day']}s)\\n"
                f"Program: {ws['lesson']}{level_str}\\n"
                f"Time: {ws['time']}\\n"
                f"Dates: {ws['start_date']} – {ws['end_date']}"
            )
            role_block_html = role_block.replace("\\n", "<br>")

            # ---- TEMPLATE 1: Handshake Campaign ----
            campaign_text = (
                f"SUBJECT: KODELY {region_upper} AFTER SCHOOL HIRING\n\n"
                f"We're Kodely, a hands-on enrichment partner delivering high-energy "
                f"after-school programs in STEM, entrepreneurship, and creative learning.\n\n"
                f"We're staffing in-person after-school teaching roles in {region_upper} "
                f"for {season}. These roles are commitment-based and require instructors "
                f"with prior experience teaching elementary-aged students.\n\n"
                f"Please read carefully before reaching out.\n"
                f"Do not apply if you cannot commit to all session dates, the exact times "
                f"listed, or if you do not have teaching experience with children.\n\n"
                f"Open Roles \u2013 {region_upper}\n"
                f"{ws['site']} ({ws['day']}s)\n"
                f"Program: {ws['lesson']}{level_str}\n"
                f"Time: {ws['time']}\n"
                f"Dates: {ws['start_date']} \u2013 {ws['end_date']}\n\n"
                f"Requirements (Mandatory)\n"
                f"\u2022 Prior teaching experience with elementary-aged children\n"
                f"\u2022 Strong classroom management and student engagement skills\n"
                f"\u2022 Reliable transportation and consistent on-time arrival\n"
                f"\u2022 Ability to commit to the full session without dropping due to "
                f"distance or schedule conflicts\n\n"
                f"If a role is accepted and later dropped, the instructor will be removed "
                f"from future placements with Kodely.\n\n"
                f"Interested?\n"
                f"Email talent@kodely.io with the subject line: {region_upper} HIRING\n"
                f"Include:\n"
                f"\u2022 The role(s) you are available for\n"
                f"\u2022 Confirmation that you can attend all listed dates and times\n"
                f"\u2022 Your resume\n"
                f"\u2022 If you require CPT/OPT\n\n"
                f"We're excited to connect with dependable educators ready to commit to "
                f"our {region_upper} programs.\n"
                f"\u2014\nKodely Team"
            )

            # ---- TEMPLATE 2: BCC Mass Email ----
            bcc_text = (
                f"SUBJECT: {region_upper} After-School Instructors Needed ({season})\n\n"
                f"We're Kodely \u2014 a hands-on, high-energy enrichment partner working "
                f"with schools to deliver engaging after-school programs in STEM, "
                f"entrepreneurship, and creative learning.\n\n"
                f"We're now staffing in-person after-school teaching roles in "
                f"{region_upper} for {season}. These roles are commitment-based and "
                f"require instructors who are reliable, experienced, and excited to work "
                f"with elementary-aged students.\n\n"
                f"Please read carefully before replying.\n"
                f"Do not apply if you cannot commit to all session dates, the exact "
                f"times listed, or if you do not have prior experience teaching children.\n\n"
                f"Open Roles \u2013 {region_upper}\n"
                f"{ws['site']} ({ws['day']}s)\n"
                f"Program: {ws['lesson']}{level_str}\n"
                f"Time: {ws['time']}\n"
                f"Dates: {ws['start_date']} \u2013 {ws['end_date']}\n\n"
                f"What We're Looking For (Required)\n"
                f"\u2022 Prior teaching experience with elementary-aged children (mandatory)\n"
                f"\u2022 Strong classroom management and student engagement skills\n"
                f"\u2022 Reliable transportation and consistent on-time arrival\n"
                f"\u2022 Flexibility and adaptability \u2014 working with kids requires it\n"
                f"\u2022 Ability to commit to the full session without dropping due to "
                f"distance or schedule conflicts\n\n"
                f"Important:\n"
                f"If a role is accepted and later dropped, the instructor will be removed "
                f"from future placements with Kodely.\n\n"
                f"Interested?\n"
                f"Only reply to this email if you can fully commit to the dates, times, "
                f"location, and have teaching experience with children. If so, we'll "
                f"schedule an interview.\n\n"
                f"We're excited to bring passionate, dependable educators into our "
                f"{region_upper} programs \u2014 and we're looking forward to connecting "
                f"with the right fit."
            )

            # ---- TEMPLATE 3: Form/Existing Leader Email ----
            form_email_text = (
                f"Hello,\n\n"
                f"We're staffing an in-person after-school role in "
                f"{region_upper}{district_str} for {season} and are reaching out to "
                f"existing Kodely instructors first.\n\n"
                f"This is a commitment-based placement. Please only respond if you can "
                f"attend every session, arrive on time, and are comfortable leading an "
                f"elementary group independently.\n\n"
                f"Available Placement \u2013 {region_upper}\n"
                f"{ws['site']} ({ws['day']}s)\n"
                f"Program: {ws['lesson']}{level_str}\n"
                f"Time: {ws['time']}\n"
                f"Dates: {ws['start_date']} \u2013 {ws['end_date']}\n\n"
                f"Requirements (Required to Confirm)\n"
                f"\u2022 Prior experience teaching elementary-aged students\n"
                f"\u2022 Strong classroom management and student engagement\n"
                f"\u2022 Reliable transportation and consistent on-time arrival\n"
                f"\u2022 Full-session commitment (no partial availability)\n\n"
                f"Please note: accepting a role and later dropping it will remove you "
                f"from future Kodely placements.\n\n"
                f"Next Steps\n"
                f"Reply to this email confirming:\n"
                f"\u2022 You can commit to all dates and times\n"
                f"\u2022 Your continued interest in this placement\n\n"
                f"We'll confirm the match once availability is verified."
            )

            def _esc(txt: str) -> str:
                return txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")

            # Candidate list for this gap
            cands_for_gap = gap_candidates.get(ws["workshop_key"], [])
            if cands_for_gap:
                cand_list_html = ""
                for c in cands_for_gap:
                    src = c.get("source", "notion")
                    badge_bg = "#e8f5e9" if src == "form" else "#e3f2fd"
                    badge_fg = "#2e7d32" if src == "form" else "#1565c0"
                    badge_label = "FORM" if src == "form" else "NOTION"
                    email_str = c.get("email") or "(none)"
                    days_str = ", ".join(sorted(c.get("available_days", [])))
                    days_part = f" | Days: {days_str}" if days_str else ""
                    date_part = f" | {c['source_date']}" if c.get("source_date") else ""
                    cand_list_html += (
                        f'<div style="padding:3px 0">'
                        f'<span style="background:{badge_bg};color:{badge_fg};padding:1px 5px;'
                        f'border-radius:3px;font-size:10px;font-weight:bold">{badge_label}</span> '
                        f'<b>{c["name"]}</b> &mdash; {email_str} '
                        f'<span style="color:#888;font-size:11px">[{c["status"]}]{days_part}{date_part}</span>'
                        f'</div>'
                    )
                match_status = f'<span style="color:#2e7d32;font-weight:bold">{len(cands_for_gap)} candidate{"s" if len(cands_for_gap)!=1 else ""} matched</span>'
            else:
                cand_list_html = '<span style="color:#c0392b;font-weight:bold">NO CANDIDATES — recruiting needed</span>'
                match_status = '<span style="color:#c0392b;font-weight:bold">0 candidates</span>'

            # Location for easy copy-paste
            location_line = f"{ws['site']}, {region_upper}"
            if ws.get("district"):
                location_line += f" ({ws['district']})"

            gap_cards += f"""
            <div style="border:2px solid {'#c0392b' if 'OPEN' in ws['gap_type'] else '#e67e22'};border-radius:8px;padding:16px;margin-bottom:20px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                <span style="font-size:16px;font-weight:bold">Gap #{gap_num}: {ws['site']}</span>
                <span style="color:{gap_color};font-weight:bold;font-size:13px">{ws['gap_type']}</span>
              </div>

              <div style="background:#f5f5f5;padding:10px;border-radius:4px;margin-bottom:12px;font-size:13px">
                <b>Location (copy-paste):</b> {location_line}<br>
                <b>Program:</b> {ws['lesson']}{level_str}<br>
                <b>Day/Time:</b> {ws['day']}s, {ws['time']}<br>
                <b>Dates:</b> {ws['start_date']} &ndash; {ws['end_date']}
              </div>

              <div style="margin-bottom:12px">
                <b>Matched Candidates:</b> {match_status}
                <div style="margin-top:4px;padding-left:8px">{cand_list_html}</div>
              </div>

              <h4 style="margin:12px 0 4px;color:#333">STEP-BY-STEP CHECKLIST</h4>
              <div style="background:#fff8e1;padding:10px;border-radius:4px;margin-bottom:12px;font-size:13px">
                <b>Step 1:</b> Email existing leaders from confirmation form (FORM candidates above)<br>
                <b>Step 2:</b> Email Notion pipeline candidates (NOTION candidates above)<br>
                <b>Step 3:</b> Post Handshake campaign (send at 9 PM, follow up next day)<br>
                <b>Step 4:</b> Send BCC mass email to broader list<br>
                <b>Step 5:</b> Check responses &amp; confirm placements
              </div>

              <details style="cursor:pointer;margin-bottom:8px">
                <summary style="color:#2e7d32;font-weight:bold;padding:6px 0">
                  &#x2709; Template 1: Email to Existing Leaders (Form Candidates)
                </summary>
                <div style="background:#f1f8e9;border:1px solid #c5e1a5;border-radius:4px;padding:12px;margin-top:6px;font-size:12px;line-height:1.6;white-space:pre-wrap;font-family:monospace">
{_esc(form_email_text)}
                </div>
              </details>

              <details style="cursor:pointer;margin-bottom:8px">
                <summary style="color:#1565c0;font-weight:bold;padding:6px 0">
                  &#x2709; Template 2: BCC Mass Email (Job Posting)
                </summary>
                <div style="background:#e3f2fd;border:1px solid #90caf9;border-radius:4px;padding:12px;margin-top:6px;font-size:12px;line-height:1.6;white-space:pre-wrap;font-family:monospace">
{_esc(bcc_text)}
                </div>
              </details>

              <details style="cursor:pointer;margin-bottom:8px">
                <summary style="color:#e65100;font-weight:bold;padding:6px 0">
                  &#x1f4e2; Template 3: Handshake Campaign (post at 9 PM)
                </summary>
                <div style="background:#fff3e0;border:1px solid #ffcc80;border-radius:4px;padding:12px;margin-top:6px;font-size:12px;line-height:1.6;white-space:pre-wrap;font-family:monospace">
{_esc(campaign_text)}
                </div>
              </details>
            </div>"""

        checklist_blocks.append(f"""
        <h2 style="color:#333;border-bottom:2px solid #455a64;padding-bottom:4px;margin-top:32px">
          {region_upper} &mdash; Action Checklist ({len(checklist_by_region[region])} gaps)
        </h2>
        {gap_cards}""")

    checklist_html = "\n".join(checklist_blocks)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:1100px;margin:0 auto;padding:16px">
  <h1 style="color:#4a90d9">Kodely Gap Match Digest</h1>
  <p style="color:#666">{today_str}</p>

  <div style="background:#f5f7fa;padding:12px 16px;border-radius:6px;margin-bottom:16px">
    <b>{total_gaps}</b> workshop gap(s) &nbsp;|&nbsp; <b>{total_candidates}</b> matched candidate(s)
  </div>
  <div style="background:#fff3e0;padding:10px 16px;border-radius:6px;margin-bottom:24px;font-size:13px">
    {heat_line}
  </div>

  <!-- HOW TO READ THIS EMAIL -->
  <div style="background:#f5f5f5;border:2px solid #bdbdbd;border-radius:8px;padding:16px;margin-bottom:24px">
    <h2 style="margin:0 0 12px;color:#333;font-size:16px">HOW TO READ THIS EMAIL</h2>

    <h3 style="margin:10px 0 6px;color:#555;font-size:14px">Ops Hub Color Coding (Leader columns)</h3>
    <table style="border-collapse:collapse;width:100%;font-size:13px;margin-bottom:12px">
      <tr style="background:#ffebee">
        <td style="padding:6px 10px;border:1px solid #ddd;width:50px"><span style="display:inline-block;width:20px;height:20px;background:#c62828;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd;width:120px"><b>Red</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Backed out &mdash; needs restaffing. Shows as <span style="color:#b71c1c;font-weight:bold">BACKOUT</span> in this digest.</td>
      </tr>
      <tr style="background:#ffebee">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:3px;background:#333;margin-top:8px;text-decoration:line-through"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b><span style="text-decoration:line-through">Strikethrough</span></b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Also backed out. Shows as <span style="color:#b71c1c;font-weight:bold">BACKOUT</span> in this digest.</td>
      </tr>
      <tr style="background:#fce4ec">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#f48fb1;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>Pink</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Scoot (3rd-party agency). We want to replace with a Kodely leader. Shows as <span style="color:#00838f;font-weight:bold">3RD PARTY (Scoot)</span>.</td>
      </tr>
      <tr style="background:#fffde7">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#fff176;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>Yellow</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Interviewing &mdash; <b>NOT a gap</b>. Candidate is being screened.</td>
      </tr>
      <tr style="background:#e8f5e9">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#81c784;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>Green</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Onboarding in progress &mdash; <b>NOT a gap</b>. Leader is being set up.</td>
      </tr>
      <tr style="background:#f3e5f5">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#ab47bc;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>Purple</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Compliance &mdash; <b>NOT a gap</b>. Background check / paperwork in progress.</td>
      </tr>
      <tr style="background:#f5f5f5">
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#9e9e9e;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>Grey</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Cancelled &mdash; entire row is <b>excluded</b> from this digest.</td>
      </tr>
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="display:inline-block;width:20px;height:20px;background:#fff;border:1px solid #ddd;border-radius:3px"></span></td>
        <td style="padding:6px 10px;border:1px solid #ddd"><b>White (no color)</b></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Confirmed leader &mdash; <b>NOT a gap</b>.</td>
      </tr>
    </table>

    <h3 style="margin:10px 0 6px;color:#555;font-size:14px">Gap Types in This Digest</h3>
    <table style="border-collapse:collapse;width:100%;font-size:13px;margin-bottom:12px">
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd;width:180px"><span style="color:#c0392b;font-weight:bold">OPEN (no leaders)</span></td>
        <td style="padding:6px 10px;border:1px solid #ddd">All leader columns are empty. No one is assigned. <b>Highest urgency.</b></td>
      </tr>
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="color:#b71c1c;font-weight:bold">BACKOUT</span></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Leader backed out (red cell or strikethrough). Needs immediate replacement.</td>
      </tr>
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="color:#00838f;font-weight:bold">3RD PARTY (Scoot)</span></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Staffed by Scoot agency (pink cell). Replace with a Kodely leader.</td>
      </tr>
    </table>

    <h3 style="margin:10px 0 6px;color:#555;font-size:14px">Candidate Sources</h3>
    <table style="border-collapse:collapse;width:100%;font-size:13px;margin-bottom:12px">
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd;width:180px"><span style="background:#e8f5e9;color:#2e7d32;padding:2px 6px;border-radius:3px;font-weight:bold">FORM</span></td>
        <td style="padding:6px 10px;border:1px solid #ddd">Applied via the Leader Confirmation Form. Date = when they submitted the form. These are people who already want to work with us.</td>
      </tr>
      <tr>
        <td style="padding:6px 10px;border:1px solid #ddd"><span style="background:#e3f2fd;color:#1565c0;padding:2px 6px;border-radius:3px;font-weight:bold">NOTION</span></td>
        <td style="padding:6px 10px;border:1px solid #ddd">In our Notion pipeline (Team Screening, Talent Screen, or Teaching Demo stage). Date = when their Notion card was created.</td>
      </tr>
    </table>

    <h3 style="margin:10px 0 6px;color:#555;font-size:14px">Sections in This Email</h3>
    <ol style="font-size:13px;line-height:1.8;margin:0;padding-left:20px">
      <li><b>Gap Tables</b> &mdash; Every gap sorted by urgency. Shows the school, workshop details, gap type, and matched candidates. Scroll through to see what needs staffing.</li>
      <li><b>Candidate Roster</b> &mdash; Full list of every matched candidate with their email, status, available days, and date. Use this to quickly copy emails.</li>
      <li><b>Action Checklist</b> &mdash; For each gap, a step-by-step to-do list with ready-to-copy-paste email templates. Click the arrows to expand each template, copy the text, and send.</li>
    </ol>
  </div>

  {no_match_note}

  <!-- SECTION 1: Gap Tables by Region -->
  <h1 style="color:#4a90d9;border-bottom:3px solid #4a90d9;padding-bottom:6px">
    Section 1: All Gaps by Region
  </h1>
  <p style="color:#666;font-size:13px;margin-bottom:16px">
    Regions with the most gaps appear first. Within each region, gaps are sorted by urgency:
    OPEN &rarr; BACKOUT &rarr; 3RD PARTY (Scoot), then by earliest start date.
  </p>
  {regions_html}

  <!-- SECTION 2: Full Candidate Roster -->
  <h1 style="color:#455a64;border-bottom:3px solid #607d8b;padding-bottom:6px;margin-top:40px">
    Section 2: All Matched Candidates
  </h1>
  <p style="color:#666;font-size:13px">
    Every candidate who matches at least one gap. <b style="background:#e8f5e9;color:#2e7d32;padding:1px 5px;border-radius:3px">FORM</b> = submitted the leader confirmation form.
    <b style="background:#e3f2fd;color:#1565c0;padding:1px 5px;border-radius:3px">NOTION</b> = in our Notion hiring pipeline.
    Only candidates active within the last 6 months are included.
  </p>
  {roster_html}

  <!-- SECTION 3: Per-Gap Action Checklist with Templates -->
  <h1 style="color:#c0392b;margin-top:48px;border-bottom:3px solid #c0392b;padding-bottom:6px">
    Section 3: Action Checklist &amp; Ready-to-Send Templates
  </h1>
  <p style="color:#666;margin-bottom:8px">
    For each gap below there are <b>3 ready-made email templates</b>. Click the arrow to expand,
    select all the text, copy it, and paste it into your email.
  </p>
  <div style="background:#ffebee;padding:14px 16px;border-radius:6px;margin-bottom:24px;font-size:13px;line-height:1.8">
    <b>FOLLOW THESE STEPS IN ORDER FOR EACH GAP:</b><br>
    <b>Step 1:</b> Email <span style="background:#e8f5e9;color:#2e7d32;padding:1px 5px;border-radius:3px;font-weight:bold">FORM</span> candidates first &mdash; they already applied, fastest to place<br>
    <b>Step 2:</b> Email <span style="background:#e3f2fd;color:#1565c0;padding:1px 5px;border-radius:3px;font-weight:bold">NOTION</span> pipeline candidates &mdash; they're in our hiring process<br>
    <b>Step 3:</b> Post the <b>Handshake campaign</b> at <b>9 PM</b>. Follow up the <b>next day</b><br>
    <b>Step 4:</b> Send the <b>BCC mass email</b> to the broader candidate list<br>
    <b>Step 5:</b> Check responses, schedule interviews, confirm placements
  </div>
  {checklist_html}

  <hr style="border:none;border-top:1px solid #ddd;margin-top:32px">
  <p style="font-size:12px;color:#999">
    Automated digest from the Interview Gap Matcher.
    Matches are based on candidate location, pipeline stage, and day availability.
    Only candidates active within the last 6 months are included.
    Grey-highlighted and cancelled rows from the Ops Hub are excluded.
  </p>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def send_email(html: str, subject: str) -> None:
    """Send the digest email via SMTP."""
    if not config.EMAILS_ENABLED:
        log.info("EMAIL PAUSED (kill switch): would send gap digest '%s'", subject)
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

    log.info("Digest email sent to %s (cc: %s)", ", ".join(to_addrs), ", ".join(cc_addrs) or "none")


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
    try:
        form_candidates = get_form_candidates(gc)
    except PermissionError:
        log.warning("No access to Form Responses sheet — skipping form candidates. "
                    "Share the sheet with the service account to include them.")
        form_candidates = []
    all_candidates = candidates + form_candidates
    workshops = get_gap_workshops(gc, creds)
    matches = find_matches(all_candidates, workshops)

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
