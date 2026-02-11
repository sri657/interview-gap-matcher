import os
from dotenv import load_dotenv

load_dotenv()

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

GOOGLE_CREDENTIALS_PATH = os.environ["GOOGLE_CREDENTIALS_PATH"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "ops-matching")

# ---------------------------------------------------------------------------
# Notion property names (from live schema)
# ---------------------------------------------------------------------------
NOTION_STATUS_PROPERTY = "Status"          # type: select
NOTION_STATUS_VALUES = [
    "\U0001f3b3 Team screening",            # 🎳 Team screening
    "Talent Screen",
    "Teaching Demo",
]
NOTION_NAME_PROPERTY = "Name"              # type: title
NOTION_LOCATION_PROPERTY = "Location"      # type: multi_select (e.g. "LA", "SF")

# ---------------------------------------------------------------------------
# Google Sheet — Kodely Workshop Ops Hub v2
# ---------------------------------------------------------------------------
SHEET_GID = int(os.environ.get("SHEET_GID", "102025870"))

# Exact column headers from the sheet
SHEET_REGION_COL = "Region"
SHEET_SITE_COL = "Site"
SHEET_DAY_COL = "Day"
SHEET_START_TIME_COL = "Start Time"
SHEET_END_TIME_COL = "End Time"
SHEET_LESSON_COL = "Lesson"
SHEET_LEAD_COL = "Lead"
SHEET_LEADER_1_COL = "Leader 1"
SHEET_LEADER_2_COL = "Leader 2"
SHEET_LEADER_3_COL = "Leader 3"

# ---------------------------------------------------------------------------
# Location normalization: maps Notion location values -> Sheet Region values
# Notion candidates may have free-text locations like "San Francisco",
# while the sheet uses short region names like "SF".
# Add mappings here as needed.
# ---------------------------------------------------------------------------
LOCATION_ALIASES = {
    "san francisco": "sf",
    "san francisco ca": "sf",
    "sf/oakland (califronia)": "sf",
    "sf/menlo park": "sf",
    "sf-bayview": "sf",
    "los angeles": "la",
    "la/east la": "la",
    "la/long beach": "la",
    "la/oc": "la",
    "la/westwood/brentwood": "la",
    "la/inglewood/calabasas": "la",
    "new york": "manhattan",
    "new york city ny": "manhattan",
    "new york ny": "manhattan",
    "nyc": "manhattan",
    "nyc area": "manhattan",
    "minnesota/minneapolis": "minnesota",
    "minneapolis": "minnesota",
    "twin cities": "minnesota",
    "san jose ca": "san jose",
    "san jose california": "san jose",
    "san diego": "san diego",
    "san deigo": "san diego",
    "denver": "colorado",
    "denver co": "colorado",
    "denver colorado": "colorado",
    "metro area denver": "colorado",
    "evanston illinois": "chicago",
    "rogers park chicago": "chicago",
    "downtown chicago": "chicago",
    "naperville": "chicago",
    "marin": "marin county",
}

# ---------------------------------------------------------------------------
# Email digest (SMTP)
# ---------------------------------------------------------------------------
EMAIL_TO = os.environ.get("EMAIL_TO", "talent@kodely.io")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")

# ---------------------------------------------------------------------------
# Google Sheet — Gap Matches output tab
# ---------------------------------------------------------------------------
SHEET_MATCHES_TAB_NAME = os.environ.get("SHEET_MATCHES_TAB_NAME", "Gap Matches")

# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------
NOTIFIED_PATH = os.path.join(os.path.dirname(__file__), "notified.json")
