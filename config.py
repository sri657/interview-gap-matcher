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
SHEET_REGION_COL = "Region"  # Col B in sheet — geographic region (SF, LA, Chicago, etc.)
SHEET_SITE_COL = "Site"
SHEET_DAY_COL = "Day"
SHEET_START_TIME_COL = "Start Time"
SHEET_END_TIME_COL = "End Time"
SHEET_LESSON_COL = "Lesson"
SHEET_LEAD_COL = "Lead"
SHEET_LEADER_1_COL = "Leader 1"
SHEET_LEADER_2_COL = "Leader 2"
SHEET_LEADER_3_COL = "Leader 3"
SHEET_DISTRICT_COL = "District"
SHEET_ZONE_COL = "Zone"
SHEET_ENROLLMENT_COL = "Enrollment"
SHEET_LEVEL_COL = "Level"

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
    # Form Responses free-text locations
    "los angeles, pasadena": "la",
    "los angeles": "la",
    "brooklyn": "brooklyn",
    "brooklyn, manhattan": "manhattan",
    "brooklyn, queens": "brooklyn",
    "brooklyn, queens, manhattan": "manhattan",
    "brooklyn, queens, the bronx, manhattan": "manhattan",
    "brooklyn, the bronx, manhattan": "manhattan",
    "brooklyn, staten island": "brooklyn",
    "dc/ maryland": "dc/maryland",
    "dc/maryland": "dc/maryland",
    "denver, boulder": "colorado",
    "denver, boulder, colorado": "colorado",
    "austin": "austin",
    "seaside ca": "sf",
    "san jose": "san jose",
    "virginia, dc/ maryland": "dc/maryland",
    "detroit, mi": "detroit",
    "chicago, il": "chicago",
    "chicago, illinois": "chicago",
    "boston": "boston",
}

# ---------------------------------------------------------------------------
# Google Sheet — Leader Confirmation Form Responses
# ---------------------------------------------------------------------------
FORM_SHEET_ID = os.environ.get(
    "FORM_SHEET_ID", "1F5-rT1K-2mUiAfXCo7batPC0opMp5Ol0jMz1XGMhoRg"
)
FORM_SHEET_GID = int(os.environ.get("FORM_SHEET_GID", "638672440"))

# Column headers in the Form Responses sheet
FORM_NAME_COL = "What is your full legal name (as it appears on your government-issued ID and background check)?\" (Please ensure this matches your official documents exactly, even if you use a different preferred name.)"
FORM_EMAIL_COL = "Email (Please only enter a personal gmail account)"
FORM_DAYS_COL = "Please share the days you're available for a 1-hour slot between 2:30–5:00 pm (you do not need to be available for the entire window)."
FORM_LOCATION_COL = "Location interested in"
FORM_DATE_COL = "Date"
FORM_STATUS_COL = "Active Status"
FORM_RETURNING_COL = "Are you a returning Kodely Leader?"

# How far back to consider candidates (months)
CANDIDATE_FRESHNESS_MONTHS = 6

# ---------------------------------------------------------------------------
# Email digest (SMTP)
# ---------------------------------------------------------------------------
EMAIL_TO = os.environ.get("EMAIL_TO", "katherine@kodely.io,arissa@kodely.io,isabella.deeb@kodely.io,servando@kodely.io,jethro@kodely.io,mitzi.yap@kodely.io")
EMAIL_CC = os.environ.get("EMAIL_CC", "sri@kodely.io")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")

# Global kill switch — set EMAILS_ENABLED=true in .env to allow outgoing emails/DMs
EMAILS_ENABLED = os.environ.get("EMAILS_ENABLED", "false").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Google Sheet — Gap Matches output tab
# ---------------------------------------------------------------------------
SHEET_MATCHES_TAB_NAME = os.environ.get("SHEET_MATCHES_TAB_NAME", "Gap Matches")

# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------
NOTIFIED_PATH = os.path.join(os.path.dirname(__file__), "notified.json")

# ---------------------------------------------------------------------------
# Onboarding tracker
# ---------------------------------------------------------------------------
ONBOARDING_DB_ID = os.environ.get("ONBOARDING_DB_ID", "b8151685f6614222a40ea7e0ca237ba9")
SLACK_ONBOARDING_CHANNEL = os.environ.get("SLACK_ONBOARDING_CHANNEL", "C09ES3F5ZCG")
SLACK_OFFBOARDING_CHANNEL = os.environ.get("SLACK_OFFBOARDING_CHANNEL", "C09F3N91G69")
ONBOARDING_MENTION_EMAILS = [
    "katherine@kodely.io",
    "arissa@kodely.io",
    "isabella.deeb@kodely.io",
    "servando@kodely.io",
    "jethro@kodely.io",
    "mitzi.yap@kodely.io",
]
ONBOARDED_PATH = os.path.join(os.path.dirname(__file__), "onboarded.json")

# ---------------------------------------------------------------------------
# Onboarding digest & compliance tracking
# ---------------------------------------------------------------------------
DIGEST_STATE_PATH = os.path.join(os.path.dirname(__file__), "digest_state.json")

# Notion onboarding DB property names (exact names from schema)
OB_COMPLIANCE_STATUS_PROPERTY = "Compliance Status"
OB_SLACK_INVITE_PROPERTY = "Invite to Slack Sent- Philippines Team"
OB_GUSTO_PROPERTY = "Added to Management Tool - Philippines Team"
OB_WORKSHOP_SLACK_PROPERTY = "Added to Workshop Slack Channel- Philippines Team"
OB_LESSON_PLAN_PROPERTY = "Lesson Plan Sent - Philippines Team"
OB_ONBOARDING_EMAIL_PROPERTY = "Onboarding Email Sent?"
OB_WORK_PERMIT_PROPERTY = "Work Permit Status"

# Values that indicate a task is complete
OB_DONE_VALUES = {"Done", "Yes", "Sent", "Approved", "Complete", "Completed", "Added", "Cleared"}

# Urgency thresholds (days before start date)
OB_URGENT_DAYS = 3
OB_WARNING_DAYS = 7

# ---------------------------------------------------------------------------
# Pipeline stages (Readiness Status values, in order)
# ---------------------------------------------------------------------------
PIPELINE_STAGES = [
    "Matched",
    "Background Check Pending",
    "Onboarding Setup",
    "Training In Progress",
    "ACTIVE",
    "Needs Review",
]

# Fields required to be complete for Onboarding Setup → Training In Progress
OB_ACCESS_FIELDS = [
    OB_SLACK_INVITE_PROPERTY,
    OB_WORKSHOP_SLACK_PROPERTY,
    OB_LESSON_PLAN_PROPERTY,
    OB_ONBOARDING_EMAIL_PROPERTY,
]

# Training status (Notion select field)
OB_TRAINING_STATUS_PROPERTY = "Training Status"
OB_TRAINING_OUTCOME_PROPERTY = "Training Outcome"

# Ops Hub cell colors (Google Sheets API RGB 0-1 scale)
CELL_COLOR_ORANGE = {"red": 1.0, "green": 0.6, "blue": 0.0}       # Matched
CELL_COLOR_PURPLE = {"red": 0.6, "green": 0.0, "blue": 1.0}       # BG Check Pending
CELL_COLOR_GREEN  = {"red": 0.0, "green": 0.8, "blue": 0.0}       # Onboarding Setup

# ---------------------------------------------------------------------------
# Calendly integration
# ---------------------------------------------------------------------------
CALENDLY_API_TOKEN = os.environ["CALENDLY_API_TOKEN"]
CALENDLY_TRAINING_EVENT_NAMES = [
    "kodely training call & dress rehearsal",
    "kodely expedited training",
    "returning leaders training/feedback call",
    "feedback/expedited training",
]
CALENDLY_STATE_PATH = os.path.join(os.path.dirname(__file__), "calendly_state.json")
TRAINING_RECENCY_DAYS = int(os.environ.get("TRAINING_RECENCY_DAYS", "90"))
CALENDLY_BOOKING_URL = "https://calendly.com/d/ck4y-gq8-nss/kodely-training-call-dress-rehearsal"

# Expedited/feedback training event names (separate from regular training)
CALENDLY_EXPEDITED_EVENT_NAMES = [
    "feedback/expedited training",
    "returning leaders training/feedback call",
]

# ---------------------------------------------------------------------------
# Training reminder report
# ---------------------------------------------------------------------------
TRAINING_REMINDER_STATE_PATH = os.path.join(os.path.dirname(__file__), "training_reminder_state.json")

# ---------------------------------------------------------------------------
# Checkr background check integration
# ---------------------------------------------------------------------------
CHECKR_API_KEY = os.environ.get("CHECKR_API_KEY", "")
CHECKR_PACKAGE = os.environ.get("CHECKR_PACKAGE", "rippling_basic_plus_package")
CHECKR_STATE_PATH = os.path.join(os.path.dirname(__file__), "checkr_state.json")

# ---------------------------------------------------------------------------
# Slack Admin / provisioning
# ---------------------------------------------------------------------------
SLACK_ADMIN_TOKEN = os.environ.get("SLACK_ADMIN_TOKEN", "")  # xoxp- token with admin.users:write scope
SLACK_TEAM_ID = os.environ.get("SLACK_TEAM_ID", "")  # Workspace team ID for admin.users.invite
SLACK_WORKSHOP_CHANNEL = os.environ.get("SLACK_WORKSHOP_CHANNEL", "")  # Default workshop channel ID
SLACK_PROVISION_STATE_PATH = os.path.join(os.path.dirname(__file__), "slack_provision_state.json")

# ---------------------------------------------------------------------------
# Welcome email links (embedded in onboarding email template)
# ---------------------------------------------------------------------------
WELCOME_CALENDLY_LINK = os.environ.get(
    "WELCOME_CALENDLY_LINK",
    "https://calendly.com/d/ck4y-gq8-nss/kodely-training-call-dress-rehearsal",
)
WELCOME_NOTION_CHECKLIST_LINK = os.environ.get("WELCOME_NOTION_CHECKLIST_LINK", "")
WELCOME_QUIZ_LINK = os.environ.get("WELCOME_QUIZ_LINK", "")
WELCOME_APP_LINK = os.environ.get(
    "WELCOME_APP_LINK",
    "https://apps.apple.com/us/app/kodely-leader/id6755861936",
)
WELCOME_ANDROID_LINK = os.environ.get(
    "WELCOME_ANDROID_LINK",
    "https://play.google.com/store/apps/details?id=com.kodely.leader.prod",
)
WELCOME_RETURNING_CALENDLY_LINK = os.environ.get(
    "WELCOME_RETURNING_CALENDLY_LINK",
    "https://calendly.com/d/cmnh-8w4-shf/returning-leaders-training-check-in",
)
WELCOME_EMAIL_STATE_PATH = os.path.join(os.path.dirname(__file__), "welcome_email_state.json")

# ---------------------------------------------------------------------------
# Anthropic API (for AI trainer notes)
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TRAINER_NOTES_STATE_PATH = os.path.join(os.path.dirname(__file__), "trainer_notes_state.json")
