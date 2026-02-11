# Interview Gap Matcher

Matches Kodely candidates in the hiring pipeline to open workshop leadership gaps, then notifies the team via Slack, email digest, and a Google Sheet tab.

## How it works

1. **Notion** — Pulls candidates at the *Team Screening*, *Talent Screen*, or *Teaching Demo* stages
2. **Google Sheets** — Reads the Workshop Ops Hub for upcoming workshops with empty or tentative leader slots
3. **Location matching** — Pairs candidates to workshops in the same region (with alias normalization)
4. **Notifications** — Posts matches to Slack, sends an HTML email digest, and writes a Gap Matches sheet tab

Each match includes school context (district, enrollment, grade level) and a Google Maps link so the team can act quickly.

## Setup

### Prerequisites

- Python 3.10+
- A Google Cloud service account with Sheets API access
- A Notion integration with access to the candidate database
- A Slack bot token with `chat:write` permission

### Install

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Create a `.env` file:

```
NOTION_API_KEY=secret_...
NOTION_DATABASE_ID=...
GOOGLE_CREDENTIALS_PATH=service-account.json
GOOGLE_SHEET_ID=...
SLACK_BOT_TOKEN=xoxb-...
SLACK_CHANNEL=ops-matching

# Optional
SHEET_GID=102025870
SHEET_MATCHES_TAB_NAME=Gap Matches
EMAIL_TO=talent@kodely.io
EMAIL_FROM=ops@kodely.io
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASSWORD=...
```

## Usage

### Match & notify via Slack

```bash
python matcher.py                    # run full match + post to Slack
python matcher.py --dry-run          # preview matches without posting
python matcher.py --region SF        # filter to a single region
```

### Email digest

```bash
python email_digest.py               # send HTML digest email
python email_digest.py --dry-run     # print HTML to stdout
```

## Project structure

| File | Purpose |
|------|---------|
| `config.py` | Environment variables, column names, location aliases |
| `matcher.py` | Core logic — Notion queries, sheet parsing, gap detection, Slack posting |
| `email_digest.py` | HTML email builder and SMTP sender |
| `write_sheet.py` | Writes match results to the Gap Matches sheet tab |

## Deduplication

Matches are tracked in `notified.json` so the same candidate+workshop pair is only posted to Slack once. The Gap Matches sheet tab is fully rewritten each run.
