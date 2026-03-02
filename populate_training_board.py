#!/usr/bin/env python3
"""
One-off: pull ALL training bookings from Calendly (past 90 days + next 60 days)
and update matching interview DB cards with:
  - Trainer Assigned
  - Date of Training
  - Status → "Training In Progress" (upcoming) or "Training Complete" (past)

Only touches interview cards that already exist (no card creation).
Safe to re-run — always overwrites with latest values.

Usage:
  cd ~/interview-gap-matcher
  ./venv/bin/python populate_training_board.py --dry-run
  ./venv/bin/python populate_training_board.py
"""

import argparse
import json
import logging
import os
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx

import config
from onboarding_tracker import NOTION_BASE, NOTION_HEADERS

# State file: dict keyed by email, stores completion record for digest
# { "email": { "name", "trainer", "date_display", "leader_type", "notion_url", "completed_at" } }
_EMAILED_STATE_PATH = os.path.join(os.path.dirname(__file__), "training_complete_emailed.json")


def _load_emailed_state() -> dict:
    try:
        with open(_EMAILED_STATE_PATH) as f:
            data = json.load(f)
            # Migrate old list format → dict
            if isinstance(data, list):
                return {email: {"completed_at": ""} for email in data}
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_emailed_state(state: dict) -> None:
    with open(_EMAILED_STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def _record_completion(
    state: dict,
    email: str,
    name: str,
    trainer: str,
    date_display: str,
    leader_type: str,
    notion_url: str,
) -> None:
    """Add or update a completion record in the state dict."""
    state[email] = {
        "name": name,
        "trainer": trainer,
        "date_display": date_display,
        "leader_type": leader_type,
        "notion_url": notion_url,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def _send_smtp(subject: str, html: str, to_addr: str) -> bool:
    """Send an HTML email via SMTP. Returns True on success."""
    if not config.EMAILS_ENABLED:
        log.info("EMAILS_ENABLED=false — skipping email")
        return False
    if not config.SMTP_USER or not config.SMTP_PASSWORD:
        log.warning("SMTP credentials not set — skipping email")
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_FROM or config.SMTP_USER
    msg["To"] = to_addr
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(msg["From"], [to_addr], msg.as_string())
        log.info("Email sent: %r → %s", subject, to_addr)
        return True
    except Exception as e:
        log.error("Failed to send email %r: %s", subject, e)
        return False


def send_digest(days: int = 1, to_addr: str = "sri@kodely.io") -> bool:
    """Send a digest of training completions newly recorded in the last `days` days.

    Only includes people whose completion was freshly detected and recorded by
    populate() (via _record_completion). Does NOT retroactively pull historical
    Training Complete cards from Notion.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    state = _load_emailed_state()
    completions: list[dict] = []

    for email, rec in state.items():
        completed_at = rec.get("completed_at", "")
        try:
            cat = datetime.fromisoformat(completed_at)
            if cat >= cutoff:
                completions.append({
                    "name": rec.get("name", email),
                    "email": email,
                    "trainer": rec.get("trainer", "—"),
                    "leader_type": rec.get("leader_type", "—"),
                    "date_display": rec.get("date_display", "—"),
                    "notion_url": rec.get("notion_url", ""),
                })
        except (ValueError, TypeError):
            pass

    if not completions:
        log.info("No training completions in the last %d day(s) — no digest sent.", days)
        return False

    # Sort by training date
    completions.sort(key=lambda x: x.get("date_display", ""))

    # --- Build email ---
    window_label = "Today" if days == 1 else f"Past {days} Days"
    subject = f"🎓 Training Complete Digest — {window_label} ({len(completions)} leader{'s' if len(completions) != 1 else ''})"

    rows = ""
    for c in completions:
        link = f'<a href="{c["notion_url"]}" style="color:#16a34a">Notion ↗</a>' if c["notion_url"] else ""
        rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{c['name']}<br>
              <span style="color:#6b7280;font-size:12px">{c['email']}</span></td>
          <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{c['trainer']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{c['date_display']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{c['leader_type']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{link}</td>
        </tr>"""

    html = f"""
<html><body style="font-family:sans-serif;font-size:14px;color:#111;max-width:700px">
  <h2 style="color:#16a34a">🎓 Training Complete — {window_label}</h2>
  <p>{len(completions)} leader{'s' if len(completions) != 1 else ''} finished training and need Gusto setup:</p>
  <table style="border-collapse:collapse;width:100%;margin-top:12px">
    <thead>
      <tr style="background:#f0fdf4;text-align:left">
        <th style="padding:8px 12px;border-bottom:2px solid #16a34a">Leader</th>
        <th style="padding:8px 12px;border-bottom:2px solid #16a34a">Trainer</th>
        <th style="padding:8px 12px;border-bottom:2px solid #16a34a">Training Date</th>
        <th style="padding:8px 12px;border-bottom:2px solid #16a34a">Type</th>
        <th style="padding:8px 12px;border-bottom:2px solid #16a34a">Card</th>
      </tr>
    </thead>
    <tbody>{rows}
    </tbody>
  </table>
  <br>
  <p><strong>Action needed:</strong> Set up Gusto for each leader above, then check <em>Added to Management Tool</em> on their Notion card.</p>
  <hr style="margin-top:24px;border:none;border-top:1px solid #e5e7eb">
  <p style="color:#6b7280;font-size:12px">Sent automatically by the Training Board sync · {datetime.now(timezone.utc).strftime("%b %d, %Y")}</p>
</body></html>
"""
    return _send_smtp(subject, html, to_addr)

# Event name → Leader Type
# "returning leaders" or "feedback" call = Returning; standard training call = New
def _leader_type_from_event(event_name: str) -> str:
    n = event_name.lower()
    if "returning" in n or "feedback" in n:
        return "Returning"
    return "New"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CALENDLY_BASE = "https://api.calendly.com"
CALENDLY_HEADERS = {
    "Authorization": f"Bearer {config.CALENDLY_API_TOKEN}",
    "Content-Type": "application/json",
}

# -------------------------------------------------------------------
# Calendly helpers
# -------------------------------------------------------------------

def get_org_uri() -> str:
    resp = httpx.get(f"{CALENDLY_BASE}/users/me", headers=CALENDLY_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()["resource"]["current_organization"]


def fetch_all_training_events(org_uri: str, days_back: int = 90, days_forward: int = 60) -> list[dict]:
    """Fetch all training events in [now - days_back, now + days_forward]."""
    now = datetime.now(timezone.utc)
    min_start = (now - timedelta(days=days_back)).isoformat()
    max_start = (now + timedelta(days=days_forward)).isoformat()

    events = []
    next_page = None

    while True:
        params = {
            "organization": org_uri,
            "min_start_time": min_start,
            "max_start_time": max_start,
            "status": "active",
            "count": 100,
        }
        if next_page:
            params["page_token"] = next_page

        resp = httpx.get(
            f"{CALENDLY_BASE}/scheduled_events",
            headers=CALENDLY_HEADERS,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for event in data.get("collection", []):
            name = event.get("name", "").lower()
            if any(term in name for term in config.CALENDLY_TRAINING_EVENT_NAMES):
                events.append(event)

        pagination = data.get("pagination", {})
        next_page = pagination.get("next_page_token")
        if not next_page:
            break

    return events


def fetch_invitees(event_uri: str) -> list[dict]:
    import time
    for attempt in range(3):
        resp = httpx.get(f"{event_uri}/invitees", headers=CALENDLY_HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            log.warning("Rate limited, waiting %ds...", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json().get("collection", [])
    return []


def event_host(event: dict) -> str:
    memberships = event.get("event_memberships", [])
    return memberships[0].get("user_name", "Unknown") if memberships else "Unknown"


# -------------------------------------------------------------------
# Notion interview DB helpers
# -------------------------------------------------------------------

def find_interview_card(email: str) -> str | None:
    """Return page_id from interview DB for this email, or None."""
    if not email:
        return None
    body = {"filter": {"property": "Email", "email": {"equals": email.lower()}}, "page_size": 1}
    resp = httpx.post(
        f"{NOTION_BASE}/databases/{config.NOTION_DATABASE_ID}/query",
        headers=NOTION_HEADERS,
        json=body,
        timeout=15,
    )
    if resp.status_code >= 400:
        return None
    results = resp.json().get("results", [])
    return results[0]["id"] if results else None


def patch_interview_card(page_id: str, properties: dict) -> bool:
    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
        timeout=15,
    )
    return resp.status_code < 400


def get_interview_card(page_id: str) -> dict | None:
    resp = httpx.get(f"{NOTION_BASE}/pages/{page_id}", headers=NOTION_HEADERS, timeout=15)
    if resp.status_code >= 400:
        return None
    return resp.json()


def get_card_status(page: dict) -> str:
    """Read the Status select value from an interview card."""
    props = page.get("properties", {})
    status_prop = props.get("Status", {})
    t = status_prop.get("type", "")
    if t == "select":
        s = status_prop.get("select")
        return s["name"] if s else ""
    return ""


# -------------------------------------------------------------------
# Main populate logic
# -------------------------------------------------------------------

def populate(dry_run: bool = False) -> None:
    now = datetime.now(timezone.utc)

    log.info("Connecting to Calendly...")
    org_uri = get_org_uri()
    log.info("Org URI: %s", org_uri)

    log.info("Fetching training events (last 90 days + next 60 days)...")
    events = fetch_all_training_events(org_uri)
    log.info("Found %d training event(s) matching training event names", len(events))

    if not events:
        log.info("No training events found.")
        return

    # Collect all bookings: (invitee_name, invitee_email, trainer, start_time, is_upcoming)
    bookings: list[dict] = []
    seen_emails: set[str] = set()  # keep only the MOST RECENT booking per person

    # Sort events newest first so we pick the latest booking per person
    events_sorted = sorted(events, key=lambda e: e.get("start_time", ""), reverse=True)

    for event in events_sorted:
        event_name = event.get("name", "")
        start_time = event.get("start_time", "")
        end_time = event.get("end_time", "")
        trainer = event_host(event)

        is_upcoming = False
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                is_upcoming = end_dt > now
            except ValueError:
                pass

        invitees = fetch_invitees(event["uri"])
        for inv in invitees:
            email = inv.get("email", "").strip().lower()
            name = inv.get("name", "").strip()
            if not email:
                continue
            if email in seen_emails:
                # Already captured a more recent booking
                continue
            seen_emails.add(email)
            bookings.append({
                "name": name,
                "email": email,
                "trainer": trainer,
                "event_name": event_name,
                "start_time": start_time,
                "is_upcoming": is_upcoming,
                "leader_type": _leader_type_from_event(event_name),
            })

    log.info("Collected %d unique invitees across all training events", len(bookings))

    updated = 0
    no_card = 0

    for b in bookings:
        email = b["email"]
        name = b["name"]
        trainer = b["trainer"]
        start_time = b["start_time"]
        is_upcoming = b["is_upcoming"]
        event_name = b["event_name"]

        page_id = find_interview_card(email)
        if not page_id:
            log.debug("No interview card for %s (%s) — skipping", name, email)
            no_card += 1
            continue

        # Read current status
        page = get_interview_card(page_id)
        current_status = get_card_status(page) if page else ""

        # Determine target status:
        # - Upcoming training → Training In Progress (unless already past that)
        # - Past training → Training Complete (unless already Active Leader)
        PROTECTED_STATUSES = {"Training Complete", "Active Leader", "Offboarded"}
        if current_status in PROTECTED_STATUSES and not is_upcoming:
            # Don't downgrade someone who's already completed/active/offboarded
            target_status = current_status
        elif is_upcoming:
            # Upcoming training → Training In Progress (unless already further along)
            target_status = current_status if current_status in PROTECTED_STATUSES else "Training In Progress"
        else:
            # Past training → Training Complete unless already Active Leader or Offboarded
            target_status = current_status if current_status in {"Active Leader", "Offboarded"} else "Training Complete"

        # Format date for display
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_display = dt.strftime("%b %d, %Y at %-I:%M %p UTC")
            date_iso = dt.isoformat()
        except (ValueError, TypeError):
            date_display = start_time
            date_iso = start_time

        if dry_run:
            status_arrow = f"{current_status or '(none)'} → {target_status}"
            print(f"{'[UPCOMING]' if is_upcoming else '[PAST]    '} {name} ({email})")
            print(f"  Event:       {event_name}")
            print(f"  Trainer:     {trainer}")
            print(f"  Date:        {date_display}")
            print(f"  Status:      {status_arrow}")
            print(f"  Leader Type: {b['leader_type']}")
            print()
            updated += 1
            continue

        # Build properties payload
        properties: dict = {
            "🎓 Trainer": {"select": {"name": trainer}},
            "🎓 Training Date": {"date": {"start": date_iso}},
            "Status": {"select": {"name": target_status}},
            "🎓 Leader Type": {"select": {"name": b["leader_type"]}},
        }

        ok = patch_interview_card(page_id, properties)
        if ok:
            log.info(
                "Updated %s (%s): %s → %s, trainer=%s, date=%s",
                name, email, current_status, target_status, trainer, date_display,
            )
            updated += 1

            # Record new Training Complete transitions for the digest (once per person)
            if target_status == "Training Complete" and current_status != "Training Complete":
                state = _load_emailed_state()
                if email not in state:
                    _record_completion(
                        state=state,
                        email=email,
                        name=name,
                        trainer=trainer,
                        date_display=date_display,
                        leader_type=b["leader_type"],
                        notion_url=_get_notion_card_url(page_id),
                    )
                    _save_emailed_state(state)
                    log.info("Recorded completion for %s in digest state", name)
        else:
            log.error("Failed to update %s (%s)", name, email)

    if dry_run:
        print(f"=== DRY RUN: {updated} would be updated, {no_card} have no interview card ===")
    else:
        log.info("Done: %d updated, %d had no interview card", updated, no_card)


def _get_notion_card_url(page_id: str) -> str:
    return f"https://www.notion.so/{page_id.replace('-', '')}"


# -------------------------------------------------------------------
# Verify: show current state of interview DB onboarding pipeline cards
# -------------------------------------------------------------------

def verify_interview_db() -> None:
    """Print all interview cards that are in onboarding pipeline stages."""
    ONBOARDING_STAGES = [
        "Matched",
        "Background Check Pending",
        "Onboarding Setup",
        "Training In Progress",
        "Training Complete",
        "Active Leader",
    ]

    print("\n=== Interview DB — Onboarding Pipeline Cards ===\n")

    for stage in ONBOARDING_STAGES:
        body = {
            "filter": {"property": "Status", "select": {"equals": stage}},
            "page_size": 100,
        }
        resp = httpx.post(
            f"{NOTION_BASE}/databases/{config.NOTION_DATABASE_ID}/query",
            headers=NOTION_HEADERS,
            json=body,
            timeout=30,
        )
        if resp.status_code >= 400:
            print(f"  [{stage}] ERROR querying: {resp.status_code}")
            continue

        results = resp.json().get("results", [])
        if not results:
            print(f"  [{stage}] — 0 cards")
            continue

        print(f"  [{stage}] — {len(results)} card(s):")
        for page in results:
            props = page.get("properties", {})

            # Name (title property)
            name = ""
            for prop in props.values():
                if prop.get("type") == "title":
                    name = "".join(t.get("plain_text", "") for t in prop.get("title", []))
                    break

            # Email
            email_prop = props.get("Email", {})
            email = email_prop.get("email", "") or ""

            # Season
            season_prop = props.get("🎓 Season", {}).get("select") or {}
            season = season_prop.get("name", "—")

            # Leader Type
            lt_prop = props.get("🎓 Leader Type", {}).get("select") or {}
            leader_type = lt_prop.get("name", "—")

            # Trainer
            tr_prop = props.get("🎓 Trainer", {}).get("select") or {}
            trainer = tr_prop.get("name", "—")

            # Training Date
            dot_prop = props.get("🎓 Training Date", {}).get("date") or {}
            dot = dot_prop.get("start", "—")
            if dot and dot != "—":
                try:
                    dot = datetime.fromisoformat(dot.replace("Z", "+00:00")).strftime("%b %d, %Y")
                except ValueError:
                    pass

            # Compliance
            cs_prop = props.get("🎓 Compliance", {}).get("select") or {}
            compliance = cs_prop.get("name", "—")

            print(f"    • {name} ({email})")
            print(f"      Season={season}  LeaderType={leader_type}  Trainer={trainer}")
            print(f"      Training Date={dot}  Compliance={compliance}")

        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate Training Board from Calendly")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--verify-only", action="store_true", help="Only show current board state, no Calendly fetch")
    parser.add_argument("--digest", action="store_true", help="Send training-complete digest email and exit")
    parser.add_argument("--days", type=int, default=1, help="Days window for --digest (default: 1)")
    args = parser.parse_args()

    if args.digest:
        sent = send_digest(days=args.days)
        if not sent:
            print("No completions in window or email disabled.")
        return

    if args.verify_only:
        verify_interview_db()
        return

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    populate(dry_run=args.dry_run)

    # Always show final board state after populating
    verify_interview_db()


if __name__ == "__main__":
    main()
