#!/usr/bin/env python3
"""
Calendly Training Sync

Polls Calendly for new training bookings, matches invitees to their Notion
onboarding cards, sets Trainer Assigned, and advances the pipeline to
"Training In Progress".
"""

import argparse
import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone, timedelta

import certifi
import httpx
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config
from onboarding_tracker import (
    _find_existing_onboarding_page,
    post_to_slack,
    NOTION_BASE,
    NOTION_HEADERS,
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

CALENDLY_BASE = "https://api.calendly.com"
CALENDLY_HEADERS = {
    "Authorization": f"Bearer {config.CALENDLY_API_TOKEN}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# State file helpers (same pattern as notified.json, onboarded.json)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.CALENDLY_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.CALENDLY_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Calendly API helpers
# ---------------------------------------------------------------------------

def get_current_user() -> dict:
    """Return the current Calendly user object (contains URI, name, etc.)."""
    resp = httpx.get(
        f"{CALENDLY_BASE}/users/me",
        headers=CALENDLY_HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["resource"]


def fetch_recent_events(org_uri: str, since_minutes: int = 1500) -> list[dict]:
    """Fetch org-wide scheduled events from the last `since_minutes` minutes.

    Default 1500 min (25 hours) to cover a full day with overlap for daily runs.
    Queries all users in the org (not just the API token owner).
    Returns only events whose name matches one of CALENDLY_TRAINING_EVENT_NAMES.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=since_minutes)).isoformat()

    params = {
        "organization": org_uri,
        "min_start_time": cutoff,
        "status": "active",
        "count": 100,
    }
    resp = httpx.get(
        f"{CALENDLY_BASE}/scheduled_events",
        headers=CALENDLY_HEADERS,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()

    training_events = []
    for event in resp.json().get("collection", []):
        event_name = event.get("name", "").lower()
        if any(term in event_name for term in config.CALENDLY_TRAINING_EVENT_NAMES):
            training_events.append(event)

    return training_events


def fetch_invitees(event_uri: str) -> list[dict]:
    """Fetch invitees for a scheduled event (with rate-limit retry)."""
    for attempt in range(3):
        resp = httpx.get(
            f"{event_uri}/invitees",
            headers=CALENDLY_HEADERS,
            timeout=30,
        )
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            log.warning("Calendly rate limited, waiting %ds (attempt %d/3)...", wait, attempt + 1)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json().get("collection", [])
    log.error("Calendly rate limit persisted after 3 retries for %s", event_uri)
    return []


def fetch_event_host(event: dict) -> str:
    """Extract the host/trainer name from event memberships."""
    memberships = event.get("event_memberships", [])
    if memberships:
        return memberships[0].get("user_name", "Unknown")
    return "Unknown"


# ---------------------------------------------------------------------------
# Training recency check
# ---------------------------------------------------------------------------

def get_last_training_date(org_uri: str, leader_email: str) -> datetime | None:
    """Query Calendly for the most recent completed training event for a leader.

    Looks back 6 months. Returns the end_time of the most recent completed
    training event, or None if no history found.
    """
    if not leader_email:
        return None

    leader_email = leader_email.strip().lower()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=180)).isoformat()

    params = {
        "organization": org_uri,
        "invitee_email": leader_email,
        "min_start_time": cutoff,
        "status": "active",
        "count": 100,
    }

    try:
        resp = httpx.get(
            f"{CALENDLY_BASE}/scheduled_events",
            headers=CALENDLY_HEADERS,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception:
        log.exception("Calendly API error looking up training history for %s", leader_email)
        return None

    latest_end: datetime | None = None
    for event in resp.json().get("collection", []):
        event_name = event.get("name", "").lower()
        if not any(term in event_name for term in config.CALENDLY_TRAINING_EVENT_NAMES):
            continue
        end_time_str = event.get("end_time", "")
        if not end_time_str:
            continue
        try:
            end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        if end_time >= now:
            continue
        if latest_end is None or end_time > latest_end:
            latest_end = end_time

    return latest_end


def is_training_recent(org_uri: str, leader_email: str) -> tuple[bool, datetime | None]:
    """Check if a leader's last training was within TRAINING_RECENCY_DAYS.

    Returns (is_recent, last_training_date).
    - (True, date) if trained within recency window
    - (False, date) if trained but too long ago
    - (False, None) if no training history found
    """
    last_date = get_last_training_date(org_uri, leader_email)
    if last_date is None:
        return False, None

    age_days = (datetime.now(timezone.utc) - last_date).days
    return age_days <= config.TRAINING_RECENCY_DAYS, last_date


# ---------------------------------------------------------------------------
# Notion: search by email fallback
# ---------------------------------------------------------------------------

def _find_page_by_email(email: str) -> tuple[str, str] | None:
    """Search the onboarding DB for a card matching the given email."""
    body = {
        "filter": {
            "property": "Email",
            "email": {"equals": email},
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
        log.warning("Notion email search failed (%s): %s", resp.status_code, resp.text)
        return None

    results = resp.json().get("results", [])
    if results:
        return results[0]["id"], results[0].get("url", "")
    return None


# ---------------------------------------------------------------------------
# Process a single booking
# ---------------------------------------------------------------------------

def process_booking(
    invitee_name: str,
    invitee_email: str,
    trainer_name: str,
    event_start: str,
    dry_run: bool = False,
    slack: SlackClient | None = None,
) -> bool:
    """Match invitee to Notion card, update trainer & status, post Slack alert.

    Returns True if processed successfully.
    """
    # --- Find the Notion onboarding card ---
    match = _find_existing_onboarding_page(invitee_name)
    match_method = "name"

    if not match and invitee_email:
        match = _find_page_by_email(invitee_email)
        match_method = "email"

    if not match:
        log.warning(
            "No Notion card found for %s (%s) — skipping", invitee_name, invitee_email
        )
        return False

    page_id, page_url = match
    log.info("Matched %s to Notion card via %s: %s", invitee_name, match_method, page_url)

    # --- Format training date for display ---
    try:
        dt = datetime.fromisoformat(event_start.replace("Z", "+00:00"))
        training_date_display = dt.strftime("%b %d, %Y at %-I:%M %p")
    except (ValueError, AttributeError):
        training_date_display = event_start

    if dry_run:
        print(f"--- DRY RUN: UPDATE NOTION CARD ---")
        print(f"  Leader: {invitee_name} ({invitee_email})")
        print(f"  Trainer Assigned: {trainer_name}")
        print(f"  Training Status: Scheduled")
        print(f"  Notion card: {page_url}")
        print(f"  Training date: {training_date_display}")
        print()
        print(f"--- DRY RUN: SLACK #ops-onboarding ---")
        print(_build_slack_message(invitee_name, trainer_name, training_date_display))
        print()
        return True

    # --- Read current card status to decide pipeline advancement ---
    card_resp = httpx.get(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        timeout=30,
    )
    current_status = None
    if card_resp.status_code < 400:
        props = card_resp.json().get("properties", {})
        readiness = props.get("Readiness Status", {}).get("select")
        if readiness:
            current_status = readiness.get("name")

    # --- Build property updates ---
    properties: dict = {
        "Trainer Assigned": {"select": {"name": trainer_name}},
        config.OB_TRAINING_STATUS_PROPERTY: {"select": {"name": "Scheduled"}},
    }

    # Advance pipeline if currently in Onboarding Setup
    if current_status == "Onboarding Setup":
        properties["Readiness Status"] = {"select": {"name": "Training In Progress"}}
        log.info("Advancing %s from Onboarding Setup → Training In Progress", invitee_name)

    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Notion update failed (%s): %s", resp.status_code, resp.text)
        return False

    log.info("Updated Notion card for %s — Trainer: %s", invitee_name, trainer_name)

    # --- Post Slack alert ---
    if slack:
        message = _build_slack_message(invitee_name, trainer_name, training_date_display)
        try:
            post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, message)
            log.info("Posted training booked alert for %s", invitee_name)
        except Exception:
            log.exception("Failed to post Slack alert for %s", invitee_name)

    return True


def _build_slack_message(leader_name: str, trainer_name: str, training_date: str) -> str:
    return (
        f":clipboard: *TRAINING BOOKED*\n\n"
        f"*Leader:* {leader_name} \u2192 Training In Progress\n"
        f"*Trainer:* {trainer_name}\n"
        f"*Training:* {training_date}"
    )


# ---------------------------------------------------------------------------
# Training completion detection
# ---------------------------------------------------------------------------

def fetch_completed_events(org_uri: str, since_hours: int = 48) -> list[dict]:
    """Fetch training events that have ended (end_time in the past).

    Looks at events from the last `since_hours` hours whose end_time < now.
    """
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=since_hours)).isoformat()

    params = {
        "organization": org_uri,
        "min_start_time": cutoff,
        "max_start_time": now.isoformat(),
        "status": "active",
        "count": 100,
    }
    resp = httpx.get(
        f"{CALENDLY_BASE}/scheduled_events",
        headers=CALENDLY_HEADERS,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()

    completed = []
    for event in resp.json().get("collection", []):
        event_name = event.get("name", "").lower()
        if not any(term in event_name for term in config.CALENDLY_TRAINING_EVENT_NAMES):
            continue
        # Only include events that have ended
        end_time_str = event.get("end_time", "")
        if end_time_str:
            try:
                end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                if end_time < now:
                    completed.append(event)
            except ValueError:
                continue

    return completed


def _query_training_in_progress() -> list[dict]:
    """Query Notion for leaders with Readiness Status = 'Training In Progress'
    and Training Status = 'Scheduled' (not yet Complete)."""
    body = {
        "filter": {
            "and": [
                {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
                {"property": config.OB_TRAINING_STATUS_PROPERTY, "select": {"equals": "Scheduled"}},
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


def _get_leader_name_from_page(page: dict) -> str:
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def check_training_completion(
    org_uri: str,
    state: dict,
    slack: SlackClient | None = None,
    dry_run: bool = False,
) -> int:
    """Check if any leaders' training events have completed.

    Matches completed Calendly events to leaders in 'Training In Progress' state
    with Training Status = 'Scheduled'. When found, marks Training Status = 'Complete'.

    Returns count of leaders marked complete.
    """
    completed_events = fetch_completed_events(org_uri)
    if not completed_events:
        log.info("No completed training events found.")
        return 0

    log.info("Found %d completed training event(s)", len(completed_events))

    # Build a set of invitee emails from completed events
    completed_invitees: dict[str, str] = {}  # email -> event end time
    for event in completed_events:
        invitees = fetch_invitees(event["uri"])
        end_time = event.get("end_time", "")
        for inv in invitees:
            email = inv.get("email", "").strip().lower()
            if email:
                completed_invitees[email] = end_time

    if not completed_invitees:
        return 0

    # Get leaders waiting for training completion
    training_leaders = _query_training_in_progress()
    log.info("Found %d leader(s) in Training In Progress with status Scheduled", len(training_leaders))

    marked = 0
    for page in training_leaders:
        page_id = page.get("id", "")
        name = _get_leader_name_from_page(page)

        # Check email match
        props = page.get("properties", {})
        email_prop = props.get("Email", {})
        leader_email = ""
        if email_prop.get("type") == "email":
            leader_email = (email_prop.get("email") or "").strip().lower()
        elif email_prop.get("type") == "rich_text":
            parts = email_prop.get("rich_text", [])
            leader_email = "".join(t.get("plain_text", "") for t in parts).strip().lower()

        completion_key = f"completed_{page_id}"
        if state.get(completion_key):
            continue

        if leader_email not in completed_invitees:
            # Fallback: try name matching against invitee names
            name_matched = False
            for event in completed_events:
                invitees = fetch_invitees(event["uri"])
                for inv in invitees:
                    inv_name = inv.get("name", "").strip().lower()
                    if inv_name and name.strip().lower() == inv_name:
                        name_matched = True
                        break
                if name_matched:
                    break

            if not name_matched:
                continue

        if dry_run:
            print(f"--- DRY RUN: TRAINING COMPLETE ---")
            print(f"  Leader: {name}")
            print(f"  Training Status: Scheduled → Complete")
            print()
            marked += 1
            continue

        # Mark Training Status = Complete
        resp = httpx.patch(
            f"{NOTION_BASE}/pages/{page_id}",
            headers=NOTION_HEADERS,
            json={"properties": {
                config.OB_TRAINING_STATUS_PROPERTY: {"select": {"name": "Complete"}},
            }},
            timeout=30,
        )
        if resp.status_code >= 400:
            log.error("Failed to mark training complete for %s: %s", name, resp.text)
            continue

        log.info("Training complete for %s — marked Complete", name)
        state[completion_key] = datetime.now(timezone.utc).isoformat()

        if slack:
            msg = (
                f":mortar_board: TRAINING COMPLETED\n\n"
                f"*Leader:* {name}\n"
                f"*Training Status:* Complete\n\n"
                f"Training event has ended — ready for pipeline advance to ACTIVE."
            )
            try:
                post_to_slack(slack, config.SLACK_ONBOARDING_CHANNEL, msg)
            except Exception:
                log.exception("Failed to post training complete alert for %s", name)

        marked += 1

    return marked


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Calendly Training Sync")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without updating Notion or posting to Slack",
    )
    args = parser.parse_args()

    # --- Connect to Calendly ---
    log.info("Connecting to Calendly...")
    user = get_current_user()
    org_uri = user["current_organization"]
    log.info("Connected as %s (org-wide search)", user.get("name", user["uri"]))

    # --- Fetch recent training events (org-wide) ---
    events = fetch_recent_events(org_uri)
    log.info("Found %d training event(s) in the last 35 minutes", len(events))

    if not events:
        log.info("No new training bookings. Exiting.")
        return

    # --- Init Slack (skip in dry-run) ---
    slack = None
    if not args.dry_run:
        slack = SlackClient(token=config.SLACK_BOT_TOKEN)

    # --- Dedup against state file ---
    state = load_state()
    processed = 0

    for event in events:
        event_uri = event["uri"]
        trainer_name = fetch_event_host(event)
        invitees = fetch_invitees(event_uri)

        for invitee in invitees:
            invitee_name = invitee.get("name", "").strip()
            invitee_email = invitee.get("email", "").strip().lower()
            dedup_key = f"{invitee_email}::{event_uri}"

            if dedup_key in state:
                log.debug("Already processed %s for event %s — skipping", invitee_email, event_uri)
                continue

            log.info("Processing booking: %s (%s) — Trainer: %s", invitee_name, invitee_email, trainer_name)

            ok = process_booking(
                invitee_name=invitee_name,
                invitee_email=invitee_email,
                trainer_name=trainer_name,
                event_start=event.get("start_time", ""),
                dry_run=args.dry_run,
                slack=slack,
            )

            if ok and not args.dry_run:
                state[dedup_key] = datetime.now(timezone.utc).isoformat()
                processed += 1

    # --- Phase 2: Check for completed training events ---
    log.info("Checking for completed training events...")
    completed_count = check_training_completion(
        org_uri, state, slack=slack, dry_run=args.dry_run,
    )

    # --- Save state ---
    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.CALENDLY_STATE_PATH)
    else:
        log.info("Dry run — state file not updated.")

    log.info(
        "Done. %d booking(s) processed, %d training(s) completed%s.",
        processed, completed_count,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
