#!/usr/bin/env python3
"""
AI Workshop Prep Notes

Uses Claude Sonnet to generate teaching tips and preparation notes for each
leader's workshop assignment. Appends a "Trainer Notes" block to the Notion
card body.

Usage:
  python trainer_notes.py --dry-run   # preview without updating Notion
  python trainer_notes.py             # generate and append notes
"""

import argparse
import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone

import certifi
import httpx
from slack_sdk import WebClient as SlackClient
from slack_sdk.errors import SlackApiError

import config

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

ANTHROPIC_BASE = "https://api.anthropic.com/v1"


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.TRAINER_NOTES_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.TRAINER_NOTES_STATE_PATH, "w") as f:
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
    if prop_type == "multi_select":
        items = prop.get("multi_select", [])
        return ", ".join(i.get("name", "") for i in items)
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in parts)
    if prop_type == "date":
        dt = prop.get("date")
        return dt.get("start", "") if dt else ""
    return ""


def _get_leader_name(page: dict) -> str:
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def query_needs_notes() -> list[dict]:
    """Query leaders in Training In Progress or Onboarding Setup."""
    body = {
        "filter": {
            "or": [
                {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                {"property": "Readiness Status", "select": {"equals": "Training In Progress"}},
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


def _get_page_blocks(page_id: str) -> list[dict]:
    """Fetch all child blocks for a page."""
    blocks = []
    cursor = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        resp = httpx.get(
            f"{NOTION_BASE}/blocks/{page_id}/children",
            headers=NOTION_HEADERS,
            params=params,
            timeout=30,
        )
        if resp.status_code >= 400:
            break
        data = resp.json()
        blocks.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return blocks


def _extract_workshop_details(blocks: list[dict]) -> list[dict]:
    """Extract workshop assignment details from page content blocks.

    Looks for "Workshop Assignment" heading blocks followed by bullet items.
    """
    workshops = []
    current_ws = None

    for block in blocks:
        block_type = block.get("type", "")

        # Detect "Workshop Assignment" heading
        if block_type == "heading_3":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("heading_3", {}).get("rich_text", [])
            )
            if "workshop assignment" in text.lower():
                current_ws = {}
                workshops.append(current_ws)
                continue

        # Parse bullet items after a workshop heading
        if block_type == "bulleted_list_item" and current_ws is not None:
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("bulleted_list_item", {}).get("rich_text", [])
            )
            if ":" in text:
                key, val = text.split(":", 1)
                current_ws[key.strip().lower()] = val.strip()

        # Dividers reset the current workshop context
        if block_type == "divider":
            current_ws = None

    return workshops


def _has_trainer_notes(blocks: list[dict]) -> bool:
    """Check if the page already has a Trainer Notes section."""
    for block in blocks:
        if block.get("type") == "heading_3":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("heading_3", {}).get("rich_text", [])
            )
            if "trainer notes" in text.lower():
                return True
    return False


def _append_trainer_notes(page_id: str, notes_text: str) -> bool:
    """Append Trainer Notes blocks to a Notion page."""
    # Split notes into paragraphs (max 2000 chars per block)
    paragraphs = [p.strip() for p in notes_text.split("\n\n") if p.strip()]

    blocks = [
        {"object": "block", "type": "divider", "divider": {}},
        {
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": "Trainer Notes (AI-Generated)"}}],
            },
        },
    ]

    for para in paragraphs:
        # Notion block text limit is 2000 chars
        for i in range(0, len(para), 2000):
            chunk = para[i:i + 2000]
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}],
                },
            })

    resp = httpx.patch(
        f"{NOTION_BASE}/blocks/{page_id}/children",
        headers=NOTION_HEADERS,
        json={"children": blocks},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Failed to append trainer notes for %s: %s %s", page_id, resp.status_code, resp.text)
        return False
    return True


# ---------------------------------------------------------------------------
# Claude AI generation
# ---------------------------------------------------------------------------

def generate_trainer_notes(
    leader_name: str,
    workshops: list[dict],
    region: str,
) -> str | None:
    """Use Claude Sonnet to generate teaching tips for a leader's workshops."""
    if not config.ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set — cannot generate trainer notes")
        return None

    # Build workshop context
    ws_lines = []
    for i, ws in enumerate(workshops, 1):
        ws_lines.append(f"Workshop {i}:")
        for key, val in ws.items():
            ws_lines.append(f"  - {key.title()}: {val}")

    if not ws_lines:
        return None

    prompt = f"""You are an experienced music education coordinator preparing a new workshop leader.
Generate concise, practical teaching tips and preparation notes for this leader.

Leader: {leader_name}
Region: {region}

{chr(10).join(ws_lines)}

Please provide:
1. Key teaching tips specific to this program/lesson type (2-3 bullet points)
2. Classroom management suggestions for the age group and school setting (2-3 bullet points)
3. Preparation checklist for their first session (3-4 items)

Keep it concise and actionable. No more than 200 words total."""

    headers = {
        "x-api-key": config.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    resp = httpx.post(
        f"{ANTHROPIC_BASE}/messages",
        headers=headers,
        json={
            "model": "claude-sonnet-4-5-20250929",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )

    if resp.status_code >= 400:
        log.error("Anthropic API error (%s): %s", resp.status_code, resp.text)
        return None

    data = resp.json()
    content_blocks = data.get("content", [])
    text_parts = [b.get("text", "") for b in content_blocks if b.get("type") == "text"]
    return "\n\n".join(text_parts) if text_parts else None


# ---------------------------------------------------------------------------
# Public API (called from onboarding_digest.py hooks)
# ---------------------------------------------------------------------------

def generate_notes_for_page(
    page: dict,
    dry_run: bool = False,
) -> bool:
    """Generate and append AI trainer notes for a single page. Returns True if notes were added."""
    page_id = page.get("id", "")
    name = _get_leader_name(page)
    region = _get_property_value(page, "Region")

    # Get existing blocks
    blocks = _get_page_blocks(page_id)

    # Skip if notes already exist
    if _has_trainer_notes(blocks):
        log.debug("Trainer notes already exist for %s — skipping", name)
        return False

    # Extract workshop details
    workshops = _extract_workshop_details(blocks)
    if not workshops:
        log.debug("No workshop assignments found for %s — skipping", name)
        return False

    if dry_run:
        print(f"--- DRY RUN: TRAINER NOTES ---")
        print(f"  Leader: {name} ({region})")
        print(f"  Workshops: {len(workshops)}")
        for i, ws in enumerate(workshops, 1):
            print(f"    {i}. {ws.get('school', ws.get('site', '?'))} — {ws.get('program', ws.get('lesson', '?'))}")
        print()
        return True

    # Generate notes via Claude
    notes = generate_trainer_notes(name, workshops, region)
    if not notes:
        log.warning("No notes generated for %s", name)
        return False

    # Append to Notion
    if _append_trainer_notes(page_id, notes):
        log.info("Trainer notes appended for %s", name)
        return True

    return False


# ---------------------------------------------------------------------------
# Main (standalone mode)
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="AI Workshop Prep Notes")
    parser.add_argument("--dry-run", action="store_true", help="Preview without updating Notion")
    args = parser.parse_args()

    if not config.ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set — exiting")
        return

    state = load_state()
    leaders = query_needs_notes()
    log.info("Found %d leader(s) in onboarding/training", len(leaders))

    generated = 0
    for page in leaders:
        page_id = page.get("id", "")
        if state.get(f"notes_{page_id}"):
            continue

        if generate_notes_for_page(page, dry_run=args.dry_run):
            state[f"notes_{page_id}"] = datetime.now(timezone.utc).isoformat()
            generated += 1

    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.TRAINER_NOTES_STATE_PATH)

    log.info(
        "Done. %d note(s) generated%s.",
        generated,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
