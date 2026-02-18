#!/usr/bin/env python3
"""
Slack Workspace & Channel Provisioning

Invites leaders to the Slack workspace via admin.users.invite and adds
them to the workshop channel via conversations.invite. Updates Notion
properties (Slack Invite + Workshop Slack) when done.

Requires a Slack user token (xoxp-) with admin.users:write scope for
workspace invites, and the bot token needs channels:manage + groups:write
for channel invites.

Usage:
  python slack_provision.py --dry-run   # preview without acting
  python slack_provision.py             # run for real
"""

import argparse
import json
import logging
import os
import re
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


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(config.SLACK_PROVISION_STATE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict) -> None:
    with open(config.SLACK_PROVISION_STATE_PATH, "w") as f:
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
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in parts)
    if prop_type == "email":
        return prop.get("email", "") or ""
    if prop_type == "rich_text":
        parts = prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in parts).strip()
    return ""


def _get_leader_name(page: dict) -> str:
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in parts)
    return ""


def _get_email(page: dict) -> str:
    props = page.get("properties", {})
    email_prop = props.get("Email", {})
    if email_prop.get("type") == "email" and email_prop.get("email"):
        return email_prop["email"]
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


def query_needs_slack(property_name: str) -> list[dict]:
    """Query leaders in Onboarding Setup who need Slack provisioning."""
    body = {
        "filter": {
            "and": [
                {"property": "Readiness Status", "select": {"equals": "Onboarding Setup"}},
                {
                    "or": [
                        {"property": property_name, "select": {"is_empty": True}},
                        {"property": property_name, "select": {"equals": "Not Done"}},
                    ],
                },
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


def _patch_notion_property(page_id: str, property_name: str, value: str) -> bool:
    resp = httpx.patch(
        f"{NOTION_BASE}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {property_name: {"select": {"name": value}}}},
        timeout=30,
    )
    if resp.status_code >= 400:
        log.error("Failed to patch %s for %s: %s %s", property_name, page_id, resp.status_code, resp.text)
        return False
    return True


# ---------------------------------------------------------------------------
# Slack API helpers
# ---------------------------------------------------------------------------

def invite_to_workspace(admin_client: SlackClient, email: str, name: str) -> bool:
    """Invite a user to the Slack workspace using admin.users.invite.

    Requires a user token (xoxp-) with admin.users:write scope.
    """
    if not config.SLACK_TEAM_ID:
        log.error("SLACK_TEAM_ID not set — cannot invite to workspace")
        return False

    first_name = name.split()[0] if name.strip() else name
    last_name = name.split()[-1] if len(name.split()) > 1 else ""

    for attempt in range(3):
        try:
            resp = admin_client.admin_users_invite(
                team_id=config.SLACK_TEAM_ID,
                email=email,
                channel_ids=config.SLACK_ONBOARDING_CHANNEL,
                real_name=name,
            )
            if resp["ok"]:
                log.info("Workspace invite sent to %s (%s)", name, email)
                return True
        except SlackApiError as e:
            error = e.response.get("error", "")
            if error == "already_invited" or error == "already_in_team":
                log.info("User %s already in workspace (or invited) — treating as success", email)
                return True
            if error == "ratelimited" and attempt < 2:
                wait = int(e.response.headers.get("Retry-After", 10))
                log.warning("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
                continue
            log.error("admin.users.invite failed for %s: %s", email, error)
            return False

    return False


def invite_to_channel(bot_client: SlackClient, email: str, channel_id: str) -> bool:
    """Add a user to a Slack channel by looking up their user ID from email.

    Uses the bot token with channels:manage / groups:write scope.
    """
    if not channel_id:
        log.warning("No channel ID provided — skipping channel invite")
        return False

    # Look up user ID
    try:
        user_resp = bot_client.users_lookupByEmail(email=email)
        if not user_resp["ok"]:
            log.warning("Could not find Slack user for %s", email)
            return False
        user_id = user_resp["user"]["id"]
    except SlackApiError as e:
        log.warning("users_lookupByEmail failed for %s: %s", email, e.response.get("error", ""))
        return False

    # Invite to channel
    for attempt in range(3):
        try:
            resp = bot_client.conversations_invite(
                channel=channel_id,
                users=user_id,
            )
            if resp["ok"]:
                log.info("Added user %s to channel %s", email, channel_id)
                return True
        except SlackApiError as e:
            error = e.response.get("error", "")
            if error == "already_in_channel":
                log.info("User %s already in channel %s", email, channel_id)
                return True
            if error == "ratelimited" and attempt < 2:
                wait = int(e.response.headers.get("Retry-After", 5))
                time.sleep(wait)
                continue
            log.error("conversations.invite failed for %s: %s", email, error)
            return False

    return False


# ---------------------------------------------------------------------------
# Slack notification helper
# ---------------------------------------------------------------------------

def _post_alert(slack: SlackClient, message: str) -> None:
    for attempt in range(3):
        try:
            slack.chat_postMessage(channel=config.SLACK_ONBOARDING_CHANNEL, text=message)
            return
        except SlackApiError as e:
            if e.response["error"] == "ratelimited" and attempt < 2:
                wait = int(e.response.headers.get("Retry-After", 5))
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Public API (called from onboarding_digest.py hooks)
# ---------------------------------------------------------------------------

def provision_slack_for_page(
    page: dict,
    bot_client: SlackClient | None = None,
    admin_client: SlackClient | None = None,
    dry_run: bool = False,
    form_emails: dict[str, str] | None = None,
) -> dict[str, bool]:
    """Provision Slack access for a single page.

    Returns {"workspace": bool, "channel": bool} indicating success.
    """
    page_id = page.get("id", "")
    name = _get_leader_name(page)
    email = _get_email(page)
    if not email and form_emails:
        from checkr_sync import _resolve_email
        email = _resolve_email(page, form_emails)
    result = {"workspace": False, "channel": False}

    if not email:
        log.warning("No email for %s — cannot provision Slack", name)
        return result

    if dry_run:
        print(f"--- DRY RUN: SLACK PROVISION ---")
        print(f"  Leader: {name} ({email})")
        print(f"  Workspace invite: admin.users.invite")
        print(f"  Channel invite: {config.SLACK_WORKSHOP_CHANNEL or '(not set)'}")
        print()
        return {"workspace": True, "channel": True}

    # Workspace invite (requires admin token)
    if admin_client:
        result["workspace"] = invite_to_workspace(admin_client, email, name)
        if result["workspace"]:
            _patch_notion_property(page_id, config.OB_SLACK_INVITE_PROPERTY, "Done")
    else:
        log.warning("No admin client — skipping workspace invite for %s", name)

    # Channel invite (requires bot token + user must be in workspace)
    if bot_client and config.SLACK_WORKSHOP_CHANNEL:
        result["channel"] = invite_to_channel(bot_client, email, config.SLACK_WORKSHOP_CHANNEL)
        if result["channel"]:
            _patch_notion_property(page_id, config.OB_WORKSHOP_SLACK_PROPERTY, "Done")
    else:
        log.warning("No bot client or workshop channel — skipping channel invite for %s", name)

    # Slack alert
    if bot_client and (result["workspace"] or result["channel"]):
        parts = []
        if result["workspace"]:
            parts.append("Workspace invite sent")
        if result["channel"]:
            parts.append("Added to workshop channel")
        msg = (
            f":busts_in_silhouette: SLACK ACCESS PROVISIONED\n\n"
            f"*Leader:* {name}\n"
            f"*Email:* {email}\n\n"
            + "\n".join(f"- {p}" for p in parts)
        )
        try:
            _post_alert(bot_client, msg)
        except Exception:
            log.exception("Failed to post Slack provision alert for %s", name)

    return result


# ---------------------------------------------------------------------------
# Main (standalone mode)
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Slack Workspace & Channel Provisioning")
    parser.add_argument("--dry-run", action="store_true", help="Preview without acting")
    args = parser.parse_args()

    bot_client = SlackClient(token=config.SLACK_BOT_TOKEN)
    admin_client = None
    if config.SLACK_ADMIN_TOKEN:
        admin_client = SlackClient(token=config.SLACK_ADMIN_TOKEN)
    else:
        log.warning("SLACK_ADMIN_TOKEN not set — workspace invites will be skipped")

    # Load form emails as fallback
    form_emails = {}
    try:
        from checkr_sync import load_form_emails
        form_emails = load_form_emails()
    except Exception:
        log.warning("Could not load form emails — will rely on Notion Email property only")

    state = load_state()

    # Find leaders needing Slack invite
    needs_workspace = query_needs_slack(config.OB_SLACK_INVITE_PROPERTY)
    needs_channel = query_needs_slack(config.OB_WORKSHOP_SLACK_PROPERTY)

    # Merge unique pages
    seen_ids = set()
    all_pages = []
    for page in needs_workspace + needs_channel:
        pid = page.get("id", "")
        if pid not in seen_ids:
            seen_ids.add(pid)
            all_pages.append(page)

    log.info("Found %d leader(s) needing Slack provisioning", len(all_pages))

    provisioned = 0
    for page in all_pages:
        page_id = page.get("id", "")
        if state.get(f"done_{page_id}"):
            continue

        result = provision_slack_for_page(
            page,
            bot_client=bot_client if not args.dry_run else None,
            admin_client=admin_client if not args.dry_run else None,
            dry_run=args.dry_run,
            form_emails=form_emails,
        )

        if result["workspace"] or result["channel"] or args.dry_run:
            state[f"done_{page_id}"] = {
                "workspace": result["workspace"],
                "channel": result["channel"],
                "at": datetime.now(timezone.utc).isoformat(),
            }
            provisioned += 1

    if not args.dry_run:
        save_state(state)
        log.info("State saved to %s", config.SLACK_PROVISION_STATE_PATH)

    log.info(
        "Done. %d leader(s) provisioned%s.",
        provisioned,
        " (dry run)" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
