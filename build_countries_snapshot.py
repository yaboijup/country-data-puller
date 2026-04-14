"""
Build a Base44-friendly JSON snapshot for multiple countries.
Output: docs/countries_snapshot.json

Run:  python build_countries_snapshot.py
Deps: pip install requests beautifulsoup4 lxml

Data strategy (March 2026):
  - Executive names/parties:    Wikipedia (free) → Claude API (fills gaps, verifies)
  - Legislature bodies/control: Claude API (smart diff, biweekly ceiling)
  - Elections:                  IPU Parline + ElectionGuide (dates) → Claude API (context, notes)
                                Daily hard refresh triggered 3 days before AND during active
                                elections (until Claude confirms a winner has been chosen).
  - Political system:           Claude API
  - Party profiles:             Claude API (updated only when a new party gains power)
  - Metadata:                   REST Countries API (live)
  - Governance:                 World Bank WGI API (live)

── RATE-LIMIT PROTECTIONS (Tier 1) ──────────────────────────────────────────

  HARD CAP PER RUN:
    MAX_CLAUDE_CALLS_PER_RUN caps the number of Claude calls in a single
    execution. Countries are prioritised:
      Priority 1 — election_watch, sentinel_alert
      Priority 2 — snapshot_anomaly, competitiveness_refresh (annual)
      Priority 3 — biweekly_tuesday_refresh, executive_name_changed, date_changed
    Once the cap is reached, remaining hard-run candidates are deferred
    (claudeDeferred=True in their entry) and picked up on the next daily run.

  ADAPTIVE SLEEP:
    CLAUDE_SLEEP_SECONDS (default 20) between every Claude call.
    CLAUDE_SLEEP_HAIKU_SECONDS (default 8) for Haiku calls.

  MODEL ROUTING:
    High-stakes calls (election_watch, sentinel_alert, snapshot_anomaly) use
    CLAUDE_MODEL_SONNET.  Low-stakes biweekly refresh calls for stable countries
    use CLAUDE_MODEL_HAIKU, which has much higher Tier-1 rate limits.

  SLIM CONTEXT:
    The previousSnapshot payload sent to Claude is stripped to only the fields
    Claude actually needs for comparison, dramatically reducing input tokens.

── NEW FEATURES ──────────────────────────────────────────────────────────────

  BIWEEKLY SCHEDULE (every other Tuesday):
    The weekly Tuesday refresh logic has been replaced with a biweekly cadence.
    The script tracks the last full-sweep date in the snapshot. If today is a
    Tuesday and it has been ≥13 days since the last full sweep, all countries
    are considered for a Claude refresh.

  ELECTION WATCH (daily hard search):
    The previous snapshot is inspected for any election within 3 days (before
    the date). Once that window opens, a hard Claude search fires every day.
    The watch continues AFTER election day as long as the snapshot marks
    electionWatchActive: true. Claude is asked to determine if a winner/
    candidate has been officially chosen. When Claude confirms the election is
    resolved, it sets electionWatchActive: false and the daily watch ends.

  CHANGE-IN-POWER SENTINEL (every 24 hours):
    At the start of each run the script fetches:
      https://stratagemdrive.github.io/change-in-power-checks/leadership-outputs.json
    Claude reads this JSON (no web search) and identifies any articles that
    signal an unexpected change in power not yet reflected in the snapshot.
    Affected countries are flagged with changeInPowerAlert in their entry;
    the flag is picked up on the next scheduled hard refresh for that country.
    Previously seen article IDs are stored in the snapshot so Claude only
    evaluates genuinely new articles.

  PARTY PROFILES:
    Each entry gains a partyProfiles block keyed by party name. Profiles
    include politicalOrientation, ideologyTags, and keyPlatforms.
    Claude only updates/adds a profile when a new party gains power in that
    country (executive or legislature change). Existing profiles are preserved.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("WARNING: beautifulsoup4 not installed. ElectionGuide scraping disabled.")
    print("         Run: pip install beautifulsoup4 lxml")

# ── CONFIG ────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5

WIKIDATA_SPARQL      = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE      = "https://api.worldbank.org/v2"
IPU_API_BASE         = "https://data.ipu.org"
IPU_PARLIAMENTS_URL  = f"{IPU_API_BASE}/api/parliaments"
IPU_ELECTIONS_URL    = f"{IPU_API_BASE}/api/elections"
REST_COUNTRIES_BASE  = "https://restcountries.com/v3.1"
WIKIPEDIA_API        = "https://en.wikipedia.org/w/api.php"
ELECTIONGUIDE_BASE   = "https://electionguide.org"

# URL for the change-in-power sentinel feed
CHANGE_IN_POWER_URL = (
    "https://stratagemdrive.github.io/change-in-power-checks/leadership-outputs.json"
)

# World Bank WGI indicators migrated to source 3 (GOV_WGI_*.SC) in 2024.
# The old VA.PER.RNK / PV.PER.RNK etc. codes are now archived (source 57)
# and return no data for many countries. The new GOV_WGI_*.SC codes return
# a 0-100 governance score that is functionally equivalent to the old
# percentile rank and uses the same 0-100 scale, so all label/tier logic
# is unchanged.  Requests must include source=3 to hit the live WGI dataset.
WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability":      "GOV_WGI_VA.SC",
    "politicalStability":       "GOV_WGI_PV.SC",
    "governmentEffectiveness":  "GOV_WGI_GE.SC",
    "regulatoryQuality":        "GOV_WGI_RQ.SC",
    "ruleOfLaw":                "GOV_WGI_RL.SC",
    "controlOfCorruption":      "GOV_WGI_CC.SC",
}

WGI_LABEL_TEMPLATES: Dict[str, Dict[str, str]] = {
    "voiceAccountability":     {"Very Low": "Very low voice & accountability",     "Low": "Low voice & accountability",     "Medium": "Moderate voice & accountability",     "High": "High voice & accountability",     "Very High": "Very high voice & accountability"},
    "politicalStability":      {"Very Low": "Very low political stability",         "Low": "Low political stability",         "Medium": "Moderate political stability",         "High": "High political stability",         "Very High": "Very high political stability"},
    "governmentEffectiveness": {"Very Low": "Very low government effectiveness",    "Low": "Low government effectiveness",    "Medium": "Moderate government effectiveness",    "High": "High government effectiveness",    "Very High": "Very high government effectiveness"},
    "regulatoryQuality":       {"Very Low": "Very low regulatory quality",          "Low": "Low regulatory quality",          "Medium": "Moderate regulatory quality",          "High": "High regulatory quality",          "Very High": "Very high regulatory quality"},
    "ruleOfLaw":               {"Very Low": "Very low rule of law",                 "Low": "Low rule of law",                 "Medium": "Moderate rule of law",                 "High": "High rule of law",                 "Very High": "Very high rule of law"},
    "controlOfCorruption":     {"Very Low": "Very low control of corruption",       "Low": "Low control of corruption",       "Medium": "Moderate control of corruption",       "High": "High control of corruption",       "Very High": "Very high control of corruption"},
}
WGI_OVERALL_LABELS: Dict[str, str] = {
    "Very Low":  "Very low governance overall",
    "Low":       "Low governance overall",
    "Medium":    "Moderate governance overall",
    "High":      "High governance overall",
    "Very High": "Very high governance overall",
}

# World Bank uses non-standard codes for some territories
WB_ISO2_OVERRIDES: Dict[str, str] = {
    "XK": "XKX",  # Kosovo — World Bank uses XKX
    "TW": "TWN",  # Taiwan — World Bank uses TWN (data may be sparse)
}

# ── RATE-LIMIT CONFIG ─────────────────────────────────────────────────────────

# Maximum Claude API calls per single script execution (Tier 1 safety cap).
# Priority 1 calls (election_watch, sentinel) are never deferred.
# Priority 2 and 3 calls are deferred once this cap is reached.
MAX_CLAUDE_CALLS_PER_RUN = int(os.environ.get("MAX_CLAUDE_CALLS_PER_RUN", "30"))

# Seconds to sleep between Claude Sonnet calls (high-stakes).
CLAUDE_SLEEP_SECONDS = int(os.environ.get("CLAUDE_SLEEP_SECONDS", "20"))

# Seconds to sleep between Claude Haiku calls (low-stakes biweekly refresh).
CLAUDE_SLEEP_HAIKU_SECONDS = int(os.environ.get("CLAUDE_SLEEP_HAIKU_SECONDS", "8"))

# Model names
CLAUDE_MODEL_SONNET = "claude-haiku-4-5-20251001"   # high-stakes
CLAUDE_MODEL_HAIKU  = "claude-haiku-4-5-20251001"   # low-stakes / biweekly refresh

# Trigger reasons that always use Sonnet regardless of cap
HIGH_STAKES_TRIGGERS = {
    "election_watch",
    "sentinel_alert",
    "snapshot_anomaly",
    "forced_refresh",
    "first_run",
}

# Call priority buckets (lower number = higher priority, never deferred first)
TRIGGER_PRIORITY: Dict[str, int] = {
    "election_watch":                1,
    "sentinel_alert":                1,
    "forced_refresh":                1,
    "first_run":                     2,
    "snapshot_anomaly":              2,
    "competitiveness_refresh":       2,
    "executive_name_changed":        3,
    "ipu_date_changed":              3,
    "eg_date_changed":               3,
    "biweekly_tuesday_refresh":      3,
}

def _trigger_priority(reason: str) -> int:
    for key, pri in TRIGGER_PRIORITY.items():
        if reason.startswith(key):
            return pri
    return 3

def _use_haiku(reason: str) -> bool:
    """Return True if this trigger is low-stakes enough for Haiku."""
    return _trigger_priority(reason) == 3

# ── COUNTRY LIST ──────────────────────────────────────────────────────────────

COUNTRIES: List[Dict[str, str]] = [
    # ── Original core ─────────────────────────────────────────────────────────
    {"country": "Ukraine",               "iso2": "UA"},
    {"country": "Russia",                "iso2": "RU"},
    {"country": "India",                 "iso2": "IN"},
    {"country": "Pakistan",              "iso2": "PK"},
    {"country": "China",                 "iso2": "CN"},
    {"country": "United Kingdom",        "iso2": "GB"},
    {"country": "Germany",               "iso2": "DE"},
    {"country": "UAE",                   "iso2": "AE"},
    {"country": "Saudi Arabia",          "iso2": "SA"},
    {"country": "Israel",                "iso2": "IL"},
    {"country": "Palestine",             "iso2": "PS"},
    {"country": "Mexico",                "iso2": "MX"},
    {"country": "Brazil",                "iso2": "BR"},
    {"country": "Canada",                "iso2": "CA"},
    {"country": "Nigeria",               "iso2": "NG"},
    {"country": "Japan",                 "iso2": "JP"},
    {"country": "Iran",                  "iso2": "IR"},
    {"country": "Syria",                 "iso2": "SY"},
    {"country": "France",                "iso2": "FR"},
    {"country": "Turkey",                "iso2": "TR"},
    {"country": "Venezuela",             "iso2": "VE"},
    {"country": "Vietnam",               "iso2": "VN"},
    {"country": "Taiwan",                "iso2": "TW"},  # See SOVEREIGNTY_NOTES
    {"country": "South Korea",           "iso2": "KR"},
    {"country": "North Korea",           "iso2": "KP"},
    {"country": "Indonesia",             "iso2": "ID"},
    {"country": "Myanmar",               "iso2": "MM"},
    {"country": "Armenia",               "iso2": "AM"},
    {"country": "Azerbaijan",            "iso2": "AZ"},
    {"country": "Morocco",               "iso2": "MA"},
    {"country": "Somalia",               "iso2": "SO"},
    {"country": "Yemen",                 "iso2": "YE"},
    {"country": "Libya",                 "iso2": "LY"},
    {"country": "Egypt",                 "iso2": "EG"},
    {"country": "Algeria",               "iso2": "DZ"},
    {"country": "Argentina",             "iso2": "AR"},
    {"country": "Chile",                 "iso2": "CL"},
    {"country": "Peru",                  "iso2": "PE"},
    {"country": "Cuba",                  "iso2": "CU"},
    {"country": "Colombia",              "iso2": "CO"},
    {"country": "Panama",                "iso2": "PA"},
    {"country": "El Salvador",           "iso2": "SV"},
    {"country": "Denmark",               "iso2": "DK"},
    {"country": "Sudan",                 "iso2": "SD"},

    # ── Europe ────────────────────────────────────────────────────────────────
    {"country": "Spain",                 "iso2": "ES"},
    {"country": "Italy",                 "iso2": "IT"},
    {"country": "Poland",                "iso2": "PL"},
    {"country": "Portugal",              "iso2": "PT"},
    {"country": "Czech Republic",        "iso2": "CZ"},
    {"country": "Norway",                "iso2": "NO"},
    {"country": "Romania",               "iso2": "RO"},
    {"country": "Sweden",                "iso2": "SE"},
    {"country": "Finland",               "iso2": "FI"},
    {"country": "Switzerland",           "iso2": "CH"},
    {"country": "Netherlands",           "iso2": "NL"},
    {"country": "Belgium",               "iso2": "BE"},
    {"country": "Ireland",               "iso2": "IE"},
    {"country": "Austria",               "iso2": "AT"},
    {"country": "Belarus",               "iso2": "BY"},
    {"country": "Hungary",               "iso2": "HU"},
    {"country": "Serbia",                "iso2": "RS"},
    {"country": "Albania",               "iso2": "AL"},
    {"country": "Bulgaria",              "iso2": "BG"},
    {"country": "Moldova",               "iso2": "MD"},
    {"country": "Greece",                "iso2": "GR"},
    {"country": "Croatia",               "iso2": "HR"},
    {"country": "Slovakia",              "iso2": "SK"},
    {"country": "Slovenia",              "iso2": "SI"},
    {"country": "Lithuania",             "iso2": "LT"},
    {"country": "Latvia",                "iso2": "LV"},
    {"country": "Estonia",               "iso2": "EE"},
    {"country": "North Macedonia",       "iso2": "MK"},
    {"country": "Bosnia and Herzegovina","iso2": "BA"},
    {"country": "Montenegro",            "iso2": "ME"},
    {"country": "Luxembourg",            "iso2": "LU"},
    {"country": "Iceland",               "iso2": "IS"},
    {"country": "Malta",                 "iso2": "MT"},
    {"country": "Cyprus",                "iso2": "CY"},
    {"country": "Georgia",               "iso2": "GE"},
    {"country": "Kosovo",                "iso2": "XK"},  # See SOVEREIGNTY_NOTES

    # ── Special Administrative Regions ────────────────────────────────────────
    {"country": "Hong Kong",             "iso2": "HK"},  # See SOVEREIGNTY_NOTES

    # ── Middle East & Central Asia ────────────────────────────────────────────
    {"country": "Iraq",                  "iso2": "IQ"},
    {"country": "Jordan",                "iso2": "JO"},
    {"country": "Lebanon",               "iso2": "LB"},
    {"country": "Kuwait",                "iso2": "KW"},
    {"country": "Bahrain",               "iso2": "BH"},
    {"country": "Oman",                  "iso2": "OM"},
    {"country": "Qatar",                 "iso2": "QA"},
    {"country": "Afghanistan",           "iso2": "AF"},
    {"country": "Turkmenistan",          "iso2": "TM"},
    {"country": "Kazakhstan",            "iso2": "KZ"},
    {"country": "Uzbekistan",            "iso2": "UZ"},
    {"country": "Kyrgyzstan",            "iso2": "KG"},
    {"country": "Tajikistan",            "iso2": "TJ"},

    # ── Asia-Pacific ──────────────────────────────────────────────────────────
    {"country": "Australia",             "iso2": "AU"},
    {"country": "New Zealand",           "iso2": "NZ"},
    {"country": "Singapore",             "iso2": "SG"},
    {"country": "Philippines",           "iso2": "PH"},
    {"country": "Malaysia",              "iso2": "MY"},
    {"country": "Thailand",              "iso2": "TH"},
    {"country": "Cambodia",              "iso2": "KH"},
    {"country": "Laos",                  "iso2": "LA"},
    {"country": "Bangladesh",            "iso2": "BD"},
    {"country": "Nepal",                 "iso2": "NP"},
    {"country": "Sri Lanka",             "iso2": "LK"},
    {"country": "Mongolia",              "iso2": "MN"},
    {"country": "Brunei",                "iso2": "BN"},
    {"country": "Timor-Leste",           "iso2": "TL"},
    {"country": "Maldives",              "iso2": "MV"},
    {"country": "Bhutan",                "iso2": "BT"},
    {"country": "Papua New Guinea",      "iso2": "PG"},

    # ── Africa ────────────────────────────────────────────────────────────────
    {"country": "Angola",                "iso2": "AO"},
    {"country": "South Africa",          "iso2": "ZA"},
    {"country": "Kenya",                 "iso2": "KE"},
    {"country": "DRC",                   "iso2": "CD"},
    {"country": "Congo",                 "iso2": "CG"},
    {"country": "Tunisia",               "iso2": "TN"},
    {"country": "Ethiopia",              "iso2": "ET"},
    {"country": "Ghana",                 "iso2": "GH"},
    {"country": "Ivory Coast",           "iso2": "CI"},
    {"country": "Senegal",               "iso2": "SN"},
    {"country": "Rwanda",                "iso2": "RW"},
    {"country": "Uganda",                "iso2": "UG"},
    {"country": "Zimbabwe",              "iso2": "ZW"},
    {"country": "Zambia",                "iso2": "ZM"},
    {"country": "Cameroon",              "iso2": "CM"},
    {"country": "Mozambique",            "iso2": "MZ"},
    {"country": "Burkina Faso",          "iso2": "BF"},
    {"country": "Niger",                 "iso2": "NE"},
    {"country": "Chad",                  "iso2": "TD"},
    {"country": "Guinea",                "iso2": "GN"},
    {"country": "Mali",                  "iso2": "ML"},
    {"country": "Botswana",              "iso2": "BW"},
    {"country": "Tanzania",              "iso2": "TZ"},
    {"country": "Madagascar",            "iso2": "MG"},
    {"country": "South Sudan",           "iso2": "SS"},
    {"country": "Eritrea",               "iso2": "ER"},
    {"country": "Djibouti",              "iso2": "DJ"},
    {"country": "Mauritania",            "iso2": "MR"},
    {"country": "Liberia",               "iso2": "LR"},
    {"country": "Sierra Leone",          "iso2": "SL"},
    {"country": "Gabon",                 "iso2": "GA"},
    {"country": "Namibia",               "iso2": "NA"},
    {"country": "Eswatini",              "iso2": "SZ"},
    {"country": "Lesotho",               "iso2": "LS"},
    {"country": "Malawi",                "iso2": "MW"},

    # ── Americas ──────────────────────────────────────────────────────────────
    {"country": "Bolivia",               "iso2": "BO"},
    {"country": "Ecuador",               "iso2": "EC"},
    {"country": "Paraguay",              "iso2": "PY"},
    {"country": "Uruguay",               "iso2": "UY"},
    {"country": "Guyana",                "iso2": "GY"},
    {"country": "Dominican Republic",    "iso2": "DO"},
    {"country": "Guatemala",             "iso2": "GT"},
    {"country": "Honduras",              "iso2": "HN"},
    {"country": "Nicaragua",             "iso2": "NI"},
    {"country": "Costa Rica",            "iso2": "CR"},
    {"country": "Haiti",                 "iso2": "HT"},
    {"country": "Trinidad and Tobago",   "iso2": "TT"},
    {"country": "Jamaica",               "iso2": "JM"},
    {"country": "Bahamas",               "iso2": "BS"},
]


# ── SPECIAL SOVEREIGNTY / STATUS NOTES ───────────────────────────────────────

SOVEREIGNTY_NOTES: Dict[str, str] = {
    "TW": (
        "Taiwan is a self-governing democracy not recognized as a sovereign state "
        "by most UN members and is not a UN or IPU member. Track its de facto "
        "government (President, Executive Yuan, Legislative Yuan) as normal, but "
        "note the contested sovereignty status in dataAvailabilityNotes."
    ),
    "HK": (
        "Hong Kong is a Special Administrative Region (SAR) of China under the "
        "'one country, two systems' framework. It has its own Chief Executive, "
        "Legislative Council (LegCo), and Basic Law, but foreign and defence policy "
        "are controlled by Beijing. Track the Chief Executive as headOfGovernment "
        "and note SAR status prominently in dataAvailabilityNotes. "
        "Hong Kong is not an IPU member."
    ),
    "XK": (
        "Kosovo declared independence from Serbia in 2008 and is recognised by "
        "roughly 100 UN member states (including the US and most EU members) but "
        "not by Serbia, Russia, China, or the UN as a whole. It is not a UN or "
        "IPU member. Track its elected government (President, Prime Minister, "
        "Assembly) as normal but note the partial-recognition status in "
        "dataAvailabilityNotes."
    ),
}

IPU_STRUCTURAL_EXCEPTIONS: set = {
    "TW",  # Taiwan    — not a UN member state, not an IPU member
    "HK",  # Hong Kong — SAR of China, not a sovereign IPU member
    "XK",  # Kosovo    — not a UN member state (partial recognition only)
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(url: str, params: Optional[dict] = None,
             headers: Optional[dict] = None, label: str = "") -> Optional[Any]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    tag = label or url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                print(f"    [req_json] {tag} → HTTP {r.status_code}")
                return None
            print(f"    [req_json] {tag} → HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            print(f"    [req_json] {tag} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
        _sleep_backoff(attempt)
    print(f"    [req_json] {tag} → all retries exhausted")
    return None

def req_html(url: str, label: str = "") -> Optional[str]:
    h = dict(HEADERS)
    h["Accept"] = "text/html,application/xhtml+xml,*/*;q=0.8"
    tag = label or url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            print(f"    [req_html] {tag} → HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            print(f"    [req_html] {tag} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
        _sleep_backoff(attempt)
    return None

def safe_get(d: Any, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def load_previous_snapshot(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {c["iso2"]: c for c in raw.get("countries", []) if c.get("iso2")}
    except Exception:
        return {}

def load_full_previous_snapshot(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

# ── SLIM CONTEXT BUILDER ──────────────────────────────────────────────────────

def _slim_prev(prev: Optional[Dict]) -> Optional[Dict]:
    """
    Return a stripped-down version of the previous snapshot to send to Claude.
    Only includes fields Claude actually needs for comparison/continuation.
    This dramatically reduces input token usage on every call.
    """
    if not prev:
        return None

    elections = prev.get("elections") or {}
    leg = elections.get("legislative") or {}
    exc = elections.get("executive") or {}

    def _slim_election(obj: Optional[Dict]) -> Optional[Dict]:
        if not obj:
            return None
        return {
            "date":  obj.get("date"),
            "type":  obj.get("type"),
            "notes": obj.get("notes"),
        }

    return {
        "executive": {
            "headOfState": {
                "name":         ((prev.get("executive") or {}).get("headOfState") or {}).get("name"),
                "partyOrGroup": ((prev.get("executive") or {}).get("headOfState") or {}).get("partyOrGroup"),
            },
            "headOfGovernment": {
                "name":         ((prev.get("executive") or {}).get("headOfGovernment") or {}).get("name"),
                "partyOrGroup": ((prev.get("executive") or {}).get("headOfGovernment") or {}).get("partyOrGroup"),
            },
        },
        "politicalSystem": (prev.get("politicalSystem") or {}).get("values"),
        "legislature": [
            {"name": b.get("name"), "inControl": b.get("inControl")}
            for b in ((prev.get("legislature") or {}).get("bodies") or [])
        ],
        "elections": {
            "competitiveElections":      elections.get("competitiveElections"),
            "nonCompetitiveReason":      elections.get("nonCompetitiveReason"),
            "electionsSuspended":        elections.get("electionsSuspended"),
            "suspensionReason":          elections.get("suspensionReason"),
            "lastCompetitivenessCheck":  elections.get("lastCompetitivenessCheck"),
            "ipu_not_applicable":        elections.get("ipu_not_applicable"),
            "ipu_not_applicable_reason": elections.get("ipu_not_applicable_reason"),
            "electionWatchActive":       elections.get("electionWatchActive"),
            "electionWatchReason":       elections.get("electionWatchReason"),
            "legislative": {
                "lastElection": _slim_election(leg.get("lastElection")),
                "nextElection": _slim_election(leg.get("nextElection")),
            },
            "executive": {
                "lastElection": _slim_election(exc.get("lastElection")),
                "nextElection": _slim_election(exc.get("nextElection")),
            },
        },
        # Keep party profiles so Claude can decide what's already profiled
        "partyProfiles": list((prev.get("partyProfiles") or {}).keys()),
        "lastClaudeUpdate": prev.get("lastClaudeUpdate"),
    }


# ── QUALITATIVE LABELS ────────────────────────────────────────────────────────

def percentile_to_tier(p: Optional[float]) -> Optional[str]:
    if p is None:
        return None
    if p < 20: return "Very Low"
    if p < 40: return "Low"
    if p < 60: return "Medium"
    if p < 80: return "High"
    return "Very High"

def percentile_to_label(p: Optional[float], dim: str) -> Optional[str]:
    tier = percentile_to_tier(p)
    return WGI_LABEL_TEMPLATES.get(dim, {}).get(tier) if tier else None

def overall_label(p: Optional[float]) -> Optional[str]:
    tier = percentile_to_tier(p)
    return WGI_OVERALL_LABELS.get(tier) if tier else None

# ── BIWEEKLY REFRESH LOGIC ─────────────────────────────────────────────────────

def _is_biweekly_tuesday(prev_full_snapshot: Dict[str, Any]) -> bool:
    today = datetime.now(timezone.utc)
    if today.weekday() != 1:
        return False
    last_sweep = prev_full_snapshot.get("lastFullSweepDate")
    if not last_sweep:
        return True
    try:
        last_dt = datetime.fromisoformat(last_sweep.replace("Z", "+00:00"))
        days_since = (today - last_dt).days
        return days_since >= 13
    except (ValueError, AttributeError):
        return True


# ── ELECTION WATCH ─────────────────────────────────────────────────────────────

def _election_watch_active(prev: Optional[Dict]) -> Tuple[bool, str]:
    if not prev:
        return False, ""

    if prev.get("elections", {}).get("electionWatchActive"):
        return True, "election_watch_carry_forward"

    today = datetime.now(timezone.utc).date()
    elec = prev.get("elections") or {}

    for block_key in ("legislative", "executive"):
        block = elec.get(block_key) or {}
        for phase_key in ("nextElection",):
            obj = block.get(phase_key)
            if not obj:
                continue
            d = obj.get("date")
            if not d:
                continue
            try:
                s = str(d)
                if len(s) == 10:
                    dt = datetime.strptime(s, "%Y-%m-%d").date()
                elif len(s) == 7:
                    y, m = s.split("-")
                    dt = datetime(int(y), int(m), 15).date()
                else:
                    continue
                days_until = (dt - today).days
                if 0 <= days_until <= 3:
                    return True, f"election_within_3_days ({d})"
                if -14 <= days_until < 0 and prev.get("elections", {}).get("electionWatchActive"):
                    return True, f"election_result_window ({d})"
            except (ValueError, AttributeError):
                continue

    return False, ""


# ── CHANGE-IN-POWER SENTINEL ───────────────────────────────────────────────────

SENTINEL_SYSTEM = """\
You are a political analyst reviewing news article summaries for unexpected \
changes in political power. You receive a JSON array of article objects. \
Each object has: id, country (ISO2), title, summary (may be null), url.

Return ONLY a valid JSON array of objects for articles that clearly indicate \
an unexpected, significant change in political leadership or power structure \
(coup, resignation, death of a leader, snap election called, government \
collapse, etc.). Each output object must have:
  { "id": <article_id>, "iso2": <country_iso2>, "alert": <1-sentence summary> }

If NO articles indicate such a change, return an empty array: []
Return ONLY the JSON array — no markdown, no explanation.\
"""


def run_change_in_power_sentinel(
    prev_full_snapshot: Dict[str, Any],
) -> Dict[str, str]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    print("\n── Change-in-Power Sentinel ──────────────────────────────────────────")

    raw = req_json(CHANGE_IN_POWER_URL, label="change-in-power sentinel feed")
    if not raw:
        print("  [SENTINEL] Failed to fetch sentinel feed — skipping")
        return {}

    articles: List[Dict] = []
    if isinstance(raw, list):
        articles = raw
    elif isinstance(raw, dict):
        articles = (raw.get("articles") or raw.get("items") or raw.get("data")
                    or raw.get("results") or [])
        if not articles:
            if raw.get("id") or raw.get("title"):
                articles = [raw]

    if not articles:
        print("  [SENTINEL] No articles found in feed")
        return {}

    print(f"  [SENTINEL] {len(articles)} article(s) in feed")

    seen_ids: set = set(prev_full_snapshot.get("sentinelSeenIds") or [])
    new_articles = [
        a for a in articles
        if isinstance(a, dict) and str(a.get("id", a.get("url", ""))) not in seen_ids
    ]

    if not new_articles:
        print("  [SENTINEL] No new articles since last run — skipping Claude call")
        return {}

    print(f"  [SENTINEL] {len(new_articles)} new article(s) to evaluate")

    if not api_key:
        print("  [SENTINEL] No ANTHROPIC_API_KEY — skipping Claude evaluation")
        return {}

    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers=headers,
            json={
                "model":      CLAUDE_MODEL_HAIKU,   # sentinel uses Haiku (low stakes)
                "max_tokens": 1000,
                "system":     SENTINEL_SYSTEM,
                "messages":   [{"role": "user", "content": json.dumps(new_articles, ensure_ascii=False)}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        raw_text = text.strip()
        raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)

        bracket_start = raw_text.find("[")
        bracket_end   = raw_text.rfind("]") + 1
        if bracket_start != -1 and bracket_end > bracket_start:
            raw_text = raw_text[bracket_start:bracket_end]

        flagged = json.loads(raw_text)
        if not isinstance(flagged, list):
            flagged = []

        alerts: Dict[str, str] = {}
        for item in flagged:
            if isinstance(item, dict) and item.get("iso2") and item.get("alert"):
                iso2 = str(item["iso2"]).upper()
                alerts[iso2] = str(item["alert"])
                print(f"  [SENTINEL] ⚠️  {iso2}: {item['alert']}")

        if not alerts:
            print("  [SENTINEL] ✓  No unexpected changes flagged")

        return alerts

    except json.JSONDecodeError as e:
        print(f"  [SENTINEL] ⚠️  JSON parse error: {e}")
    except requests.HTTPError as e:
        print(f"  [SENTINEL] ⚠️  Claude HTTP error: {e}")
    except Exception as e:
        print(f"  [SENTINEL] ⚠️  Error: {e}")

    return {}


def update_sentinel_seen_ids(
    prev_full_snapshot: Dict[str, Any],
    current_feed_articles: List[Dict],
) -> List[str]:
    seen: set = set(prev_full_snapshot.get("sentinelSeenIds") or [])
    for a in current_feed_articles:
        if isinstance(a, dict):
            art_id = str(a.get("id", a.get("url", "")))
            if art_id:
                seen.add(art_id)
    return sorted(seen)


# ── WIKIPEDIA ADAPTIVE EXECUTIVE LOOKUP ──────────────────────────────────────

_wiki_exec_cache: Optional[Dict[str, Dict[str, Optional[str]]]] = None

def _load_wiki_exec_cache() -> Dict[str, Dict[str, Optional[str]]]:
    global _wiki_exec_cache
    if _wiki_exec_cache is not None:
        return _wiki_exec_cache

    print("  [WIKI] Fetching Wikipedia heads of state/government list...")

    from html.parser import HTMLParser

    params = {
        "action": "parse",
        "page": "List of current heads of state and government",
        "prop": "text",
        "format": "json",
        "formatversion": "2",
        "disableeditsection": "1",
    }
    data = req_json(WIKIPEDIA_API, params=params, label="Wikipedia HOS/HOG list")
    if not data:
        print("  [WIKI] Failed — using static data only")
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    html_text = safe_get(data, "parse", "text", default="")
    if not html_text:
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    result: Dict[str, Dict[str, Optional[str]]] = {}

    class TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.in_cell = False
            self.current_row: List[str] = []
            self.current_cell_parts: List[str] = []
            self.rows: List[List[str]] = []

        def _cell_text(self) -> str:
            raw = " ".join(self.current_cell_parts).strip()
            raw = re.sub(r"\[\d+\]", "", raw)
            raw = re.sub(r"\s+", " ", raw).strip()
            return raw

        def handle_starttag(self, tag, attrs):
            attrs_dict = dict(attrs)
            if tag == "table" and "wikitable" in attrs_dict.get("class", ""):
                self.in_table = True
            if not self.in_table:
                return
            if tag == "tr":
                self.current_row = []
            if tag in ("td", "th"):
                self.in_cell = True
                self.current_cell_parts = []
            if tag == "br":
                self.current_cell_parts.append(" | ")

        def handle_endtag(self, tag):
            if not self.in_table:
                return
            if tag in ("td", "th") and self.in_cell:
                self.current_row.append(self._cell_text())
                self.in_cell = False
                self.current_cell_parts = []
            if tag == "tr" and self.current_row:
                self.rows.append(self.current_row)
                self.current_row = []
            if tag == "table":
                self.in_table = False

        def handle_data(self, data):
            if self.in_cell:
                self.current_cell_parts.append(data)

        def handle_entityref(self, name):
            if self.in_cell:
                entities = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ",
                            "ndash": "–", "mdash": "—"}
                self.current_cell_parts.append(entities.get(name, ""))

        def handle_charref(self, name):
            if self.in_cell:
                try:
                    c = chr(int(name[1:], 16) if name.startswith("x") else int(name))
                    self.current_cell_parts.append(c)
                except Exception:
                    pass

    parser = TableParser()
    parser.feed(html_text)

    WIKI_NAME_MAP = {
        "RU": "Russia", "IN": "India", "PK": "Pakistan", "CN": "China",
        "GB": "United Kingdom", "DE": "Germany", "AE": "United Arab Emirates",
        "SA": "Saudi Arabia", "IL": "Israel", "PS": "Palestine", "MX": "Mexico",
        "BR": "Brazil", "CA": "Canada", "NG": "Nigeria", "JP": "Japan",
        "IR": "Iran", "SY": "Syria", "FR": "France", "TR": "Turkey",
        "VE": "Venezuela", "VN": "Vietnam", "KR": "South Korea", "KP": "North Korea",
        "ID": "Indonesia", "MM": "Myanmar", "AM": "Armenia", "AZ": "Azerbaijan",
        "MA": "Morocco", "SO": "Somalia", "YE": "Yemen", "LY": "Libya",
        "EG": "Egypt", "DZ": "Algeria", "AR": "Argentina", "CL": "Chile",
        "PE": "Peru", "CU": "Cuba", "CO": "Colombia", "PA": "Panama",
        "SV": "El Salvador", "DK": "Denmark", "SD": "Sudan", "UA": "Ukraine",
        "AU": "Australia", "SG": "Singapore", "PH": "Philippines", "AF": "Afghanistan",
        "IQ": "Iraq", "ES": "Spain", "IT": "Italy", "PL": "Poland", "BO": "Bolivia",
        "NZ": "New Zealand", "PT": "Portugal", "CZ": "Czech Republic", "NO": "Norway",
        "RO": "Romania", "SE": "Sweden", "FI": "Finland", "CH": "Switzerland",
        "NL": "Netherlands", "BE": "Belgium", "IE": "Ireland", "AT": "Austria",
        "BY": "Belarus", "HU": "Hungary", "RS": "Serbia", "AL": "Albania",
        "BG": "Bulgaria", "MD": "Moldova", "GR": "Greece", "HR": "Croatia",
        "SK": "Slovakia", "SI": "Slovenia", "LT": "Lithuania", "LV": "Latvia",
        "EE": "Estonia", "MK": "North Macedonia", "BA": "Bosnia and Herzegovina",
        "ME": "Montenegro", "LU": "Luxembourg", "IS": "Iceland", "MT": "Malta",
        "CY": "Cyprus", "GE": "Georgia", "HK": "Hong Kong", "XK": "Kosovo",
        "OM": "Oman", "QA": "Qatar", "JO": "Jordan", "LB": "Lebanon",
        "KW": "Kuwait", "BH": "Bahrain", "TM": "Turkmenistan", "KZ": "Kazakhstan",
        "UZ": "Uzbekistan", "KG": "Kyrgyzstan", "TJ": "Tajikistan",
        "MY": "Malaysia", "TH": "Thailand", "KH": "Cambodia", "LA": "Laos",
        "BD": "Bangladesh", "NP": "Nepal", "LK": "Sri Lanka", "MN": "Mongolia",
        "BN": "Brunei", "TL": "Timor-Leste", "MV": "Maldives", "BT": "Bhutan",
        "PG": "Papua New Guinea", "AO": "Angola", "ZA": "South Africa",
        "KE": "Kenya", "CD": "Democratic Republic of the Congo",
        "CG": "Republic of the Congo", "TN": "Tunisia", "ET": "Ethiopia",
        "GH": "Ghana", "CI": "Ivory Coast", "SN": "Senegal", "RW": "Rwanda",
        "UG": "Uganda", "ZW": "Zimbabwe", "ZM": "Zambia", "CM": "Cameroon",
        "MZ": "Mozambique", "BF": "Burkina Faso", "NE": "Niger", "TD": "Chad",
        "GN": "Guinea", "ML": "Mali", "BW": "Botswana", "TZ": "Tanzania",
        "MG": "Madagascar", "SS": "South Sudan", "ER": "Eritrea", "DJ": "Djibouti",
        "MR": "Mauritania", "LR": "Liberia", "SL": "Sierra Leone", "GA": "Gabon",
        "NA": "Namibia", "SZ": "Eswatini", "LS": "Lesotho", "MW": "Malawi",
        "EC": "Ecuador", "PY": "Paraguay", "UY": "Uruguay", "GY": "Guyana",
        "DO": "Dominican Republic", "GT": "Guatemala", "HN": "Honduras",
        "NI": "Nicaragua", "CR": "Costa Rica", "HT": "Haiti",
        "TT": "Trinidad and Tobago", "JM": "Jamaica", "BS": "Bahamas",
    }
    rev = {v.lower(): k for k, v in WIKI_NAME_MAP.items()}

    def _first_name(s: str) -> Optional[str]:
        if not s:
            return None
        parts = [p.strip() for p in s.split("|") if p.strip()]
        return (parts[0] if parts else s).strip() or None

    for row in parser.rows:
        if len(row) < 2:
            continue
        country_raw = row[0].strip()
        if not country_raw or country_raw.lower() in ("country", "state", ""):
            continue
        iso2 = rev.get(country_raw.lower())
        if not iso2:
            for wiki_lower, code in rev.items():
                if wiki_lower in country_raw.lower() or country_raw.lower() in wiki_lower:
                    iso2 = code
                    break
        if not iso2:
            continue
        hos_raw = row[1].strip() if len(row) > 1 else ""
        hog_raw = row[2].strip() if len(row) > 2 else hos_raw
        result[iso2] = {
            "hosName": _first_name(hos_raw),
            "hogName": _first_name(hog_raw),
        }

    print(f"  [WIKI] Parsed {len(result)} countries")
    _wiki_exec_cache = result
    return result


# ── IPU PARLINE ───────────────────────────────────────────────────────────────

_ipu_parliament_map: Optional[Dict[str, Dict]] = None

def _load_ipu_parliament_map() -> Dict[str, Dict]:
    global _ipu_parliament_map
    if _ipu_parliament_map is not None:
        return _ipu_parliament_map

    print("  [IPU] Loading parliament list from IPU Parline API...")
    _ipu_parliament_map = {}

    page = 1
    per_page = 100
    total_loaded = 0

    while True:
        params = {"page": page, "per_page": per_page, "format": "json"}
        data = req_json(
            f"{IPU_API_BASE}/api/parliaments",
            params=params,
            headers={"Accept": "application/json"},
            label=f"IPU /api/parliaments page {page}",
        )
        if not data:
            print(f"  [IPU] Failed to load parliament list page {page}")
            break

        records: List[Dict] = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data") or data.get("results") or data.get("parliaments") or []
            if not records and "id" in data:
                records = [data]

        if not records:
            break

        for rec in records:
            if not isinstance(rec, dict):
                continue
            country = rec.get("country") or rec.get("countryCode") or {}
            iso2 = None
            if isinstance(country, dict):
                iso2 = (country.get("isoCode") or country.get("iso2") or
                        country.get("code") or "").upper()
            elif isinstance(country, str):
                iso2 = country.upper()
            if not iso2:
                iso2 = (rec.get("isoCode") or rec.get("iso2") or
                        rec.get("country_code") or "").upper()
            if iso2 and len(iso2) == 2:
                if iso2 not in _ipu_parliament_map:
                    _ipu_parliament_map[iso2] = rec
                total_loaded += 1

        print(f"  [IPU] Page {page}: loaded {len(records)} records, {len(_ipu_parliament_map)} unique countries so far")

        if len(records) < per_page:
            break

        page += 1
        time.sleep(0.3)

    print(f"  [IPU] Parliament map loaded: {len(_ipu_parliament_map)} countries")
    return _ipu_parliament_map


def _get_ipu_elections_for_country(iso2: str) -> List[Dict]:
    parl_map = _load_ipu_parliament_map()
    parl = parl_map.get(iso2.upper())

    if not parl:
        return []

    parl_id = parl.get("id") or parl.get("parliamentId") or parl.get("parliament_id")
    if not parl_id:
        return []

    print(f"    [IPU] Fetching elections for {iso2} (parliament ID: {parl_id})")

    params = {
        "parliament": parl_id,
        "format": "json",
        "per_page": 20,
        "sort": "date_desc",
    }
    data = req_json(
        f"{IPU_API_BASE}/api/elections",
        params=params,
        headers={"Accept": "application/json"},
        label=f"IPU /api/elections?parliament={parl_id}",
    )

    if not data:
        return []

    records: List[Dict] = []
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get("data") or data.get("results") or data.get("elections") or []

    return [r for r in records if isinstance(r, dict)]


def _parse_ipu_date(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("date") or raw.get("text")
    if not raw:
        return None
    s = str(raw).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s): return s
    if re.match(r"^\d{4}-\d{2}$", s):        return s
    if re.match(r"^\d{4}$", s):               return s
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T", s)
    if m: return m.group(1)
    return s or None


def _extract_ipu_election_date(rec: Dict) -> Optional[str]:
    for field in ("date", "electionDate", "election_date", "dateOfElection",
                  "lastElectionDate", "date_of_election"):
        val = rec.get(field)
        if val:
            d = _parse_ipu_date(val)
            if d:
                return d
    return None


def _classify_ipu_election(rec: Dict) -> str:
    etype = (rec.get("electionType") or rec.get("election_type") or
             rec.get("type") or rec.get("round") or "")
    if isinstance(etype, dict):
        etype = etype.get("label") or etype.get("value") or ""
    etype = str(etype).lower()

    is_snap    = rec.get("isSnap") or rec.get("is_snap") or "snap" in etype or "early" in etype
    is_runoff  = (rec.get("isRunoff") or rec.get("round2") or rec.get("second_round")
                  or "runoff" in etype or "second round" in etype or "2nd round" in etype
                  or "(2)" in etype)
    is_by      = "by-election" in etype or "by_election" in etype or "byelection" in etype
    is_extra   = "extraordinary" in etype or "special" in etype

    body = (rec.get("parliamentName") or rec.get("parliament_name") or
            rec.get("chamberName") or rec.get("body") or "Parliamentary")
    if isinstance(body, dict):
        body = body.get("label") or body.get("value") or "Parliamentary"

    if is_runoff:
        return f"{body} (runoff)"
    if is_snap:
        return f"{body} (snap election)"
    if is_by:
        return f"{body} (by-election)"
    if is_extra:
        return f"{body} (extraordinary)"
    return str(body)


def fetch_ipu_elections(iso2: str, prev: Optional[Dict] = None) -> Dict[str, Any]:
    iso = iso2.upper()

    if iso in IPU_STRUCTURAL_EXCEPTIONS:
        reason_map = {
            "TW": "Taiwan is not an IPU member (non-UN member state).",
            "HK": "Hong Kong is a SAR of China and is not a sovereign IPU member.",
            "XK": "Kosovo is not an IPU member (partial UN recognition only).",
        }
        return {"lastDate": None, "nextDate": None, "elections": [],
                "source": "ipu_not_applicable",
                "notes": reason_map.get(iso, "IPU not applicable (structural exception).")}

    if prev:
        prev_elec = prev.get("elections") or {}
        if prev_elec.get("ipu_not_applicable"):
            reason = prev_elec.get("ipu_not_applicable_reason") or "IPU not applicable (set by Claude)."
            return {"lastDate": None, "nextDate": None, "elections": [],
                    "source": "ipu_not_applicable", "notes": reason}

    elections = _get_ipu_elections_for_country(iso2)
    if not elections:
        return {"lastDate": None, "nextDate": None, "elections": [],
                "source": "ipu_no_data",
                "notes": f"IPU Parline returned no elections for {iso2}."}

    today = datetime.now(timezone.utc).date()
    past_dates: List[str] = []
    future_dates: List[str] = []

    for rec in elections:
        d = _extract_ipu_election_date(rec)
        if not d:
            continue
        try:
            if len(d) == 4:
                dt = datetime(int(d), 12, 31).date()
            elif len(d) == 7:
                y, m = d.split("-")
                dt = datetime(int(y), int(m), 28).date()
            else:
                dt = datetime.strptime(d, "%Y-%m-%d").date()

            if dt <= today:
                past_dates.append(d)
            else:
                future_dates.append(d)
        except ValueError:
            past_dates.append(d)

    last_date = max(past_dates) if past_dates else None
    next_date = min(future_dates) if future_dates else None

    next_record = None
    if future_dates:
        earliest_next = min(future_dates)
        for rec in elections:
            if _extract_ipu_election_date(rec) == earliest_next:
                next_record = rec
                break

    return {
        "lastDate": last_date,
        "nextDate": next_date,
        "nextType": _classify_ipu_election(next_record) if next_record else None,
        "elections": elections[:5],
        "source": "ipu_parline",
        "notes": f"IPU Parline: {len(elections)} election record(s) found.",
    }


# ── ELECTIONGUIDE SCRAPER ─────────────────────────────────────────────────────

_eg_cache: Optional[Dict[str, List[Dict]]] = None

def _load_electionguide_cache() -> Dict[str, List[Dict]]:
    global _eg_cache
    if _eg_cache is not None:
        return _eg_cache

    _eg_cache = {}

    if not BS4_AVAILABLE:
        print("  [EG] beautifulsoup4 not available, skipping ElectionGuide scrape")
        return _eg_cache

    EG_NAME_OVERRIDES: Dict[str, str] = {
        "United Kingdom of Great Britain and Northern Ireland": "GB",
        "United Arab Emirates": "AE", "Korea, Republic of": "KR",
        "Korea (North)": "KP", "Korea, Democratic People's Republic of": "KP",
        "Viet Nam": "VN", "Vietnam": "VN", "Iran, Islamic Republic of": "IR",
        "Syrian Arab Republic": "SY", "Bolivia, Plurinational State of": "BO",
        "Venezuela, Bolivarian Republic of": "VE", "Congo (Brazzaville)": "CG",
        "Congo, Democratic Republic of the": "CD", "Congo (Kinshasa)": "CD",
        "Democratic Republic of the Congo": "CD", "Republic of the Congo": "CG",
        "Türkiye": "TR", "Turkey": "TR", "Russian Federation": "RU",
        "Republic of Korea": "KR", "Czechia": "CZ", "Czech Republic": "CZ",
        "Ivory Coast": "CI", "Côte d'Ivoire": "CI", "Eswatini": "SZ",
        "Swaziland": "SZ", "North Macedonia": "MK", "Macedonia": "MK",
        "Bosnia and Herzegovina": "BA", "Bosnia & Herzegovina": "BA",
        "Trinidad and Tobago": "TT", "Trinidad & Tobago": "TT",
        "Timor-Leste": "TL", "East Timor": "TL", "Papua New Guinea": "PG",
        "Dominican Republic": "DO", "El Salvador": "SV", "South Korea": "KR",
        "North Korea": "KP", "South Sudan": "SS", "Hong Kong": "HK",
        "Kosovo": "XK", "Laos": "LA", "Lao People's Democratic Republic": "LA",
        "Myanmar": "MM", "Burma": "MM", "Burkina Faso": "BF",
        "Sierra Leone": "SL", "Sri Lanka": "LK", "New Zealand": "NZ",
        "Saudi Arabia": "SA", "South Africa": "ZA",
    }

    country_name_to_iso2: Dict[str, str] = {c["country"].lower(): c["iso2"] for c in COUNTRIES}
    for eg_name, iso2 in EG_NAME_OVERRIDES.items():
        country_name_to_iso2[eg_name.lower()] = iso2

    def _name_to_iso2(name: str) -> Optional[str]:
        clean = name.strip().lower()
        if clean in country_name_to_iso2:
            return country_name_to_iso2[clean]
        for known, code in country_name_to_iso2.items():
            if known in clean or clean in known:
                return code
        return None

    def _parse_eg_page(url: str, status: str) -> None:
        print(f"  [EG] Scraping {url}")
        html = req_html(url, label=f"ElectionGuide {status}")
        if not html:
            print(f"  [EG] Failed to fetch {url}")
            return

        soup = BeautifulSoup(html, "lxml")
        parsed_count = 0

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            date_text = ""
            body_text = ""
            country_text = ""

            for cell in cells:
                text = cell.get_text(separator=" ", strip=True)
                date_m = re.search(
                    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4}",
                    text,
                )
                if date_m and not date_text:
                    date_text = date_m.group(0)
                for a in cell.find_all("a"):
                    href = a.get("href", "")
                    link_text = a.get_text(strip=True)
                    if "/elections/id/" in href and link_text and not body_text:
                        body_text = link_text
                    elif "/countries/id/" in href and link_text and not country_text:
                        country_text = link_text

            if not (date_text and country_text):
                continue

            try:
                dt = datetime.strptime(date_text, "%b %d %Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                iso_date = date_text

            iso2 = _name_to_iso2(country_text)
            if not iso2:
                continue

            if iso2 not in _eg_cache:
                _eg_cache[iso2] = []
            _eg_cache[iso2].append({
                "date": iso_date,
                "body": body_text,
                "country": country_text,
                "status": status,
            })
            parsed_count += 1

        print(f"  [EG] Parsed {parsed_count} elections from {url}")
        time.sleep(0.5)

    _parse_eg_page(f"{ELECTIONGUIDE_BASE}/elections/type/past/", "past")
    _parse_eg_page(f"{ELECTIONGUIDE_BASE}/elections/type/upcoming/", "upcoming")

    total = sum(len(v) for v in _eg_cache.values())
    print(f"  [EG] Cache complete: {total} elections across {len(_eg_cache)} countries")
    return _eg_cache


def get_electionguide_dates(iso2: str) -> Dict[str, Optional[str]]:
    cache = _load_electionguide_cache()
    records = cache.get(iso2.upper(), [])
    if not records:
        return {"lastDate": None, "nextDate": None, "source": "electionguide_no_data"}

    today = datetime.now(timezone.utc).date()
    past: List[str] = []
    future: List[str] = []

    for rec in records:
        d = rec.get("date", "")
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            if dt <= today:
                past.append(d)
            else:
                future.append(d)
        except ValueError:
            past.append(d)

    next_record = None
    if future:
        earliest = min(future)
        next_record = next((r for r in records if r.get("date") == earliest), None)

    def _eg_classify(rec: Optional[Dict]) -> Optional[str]:
        if not rec:
            return None
        body = rec.get("body", "")
        b = body.lower()
        if "runoff" in b or "2nd round" in b or "second round" in b or "(2)" in b:
            return f"{body} (runoff)"
        if "snap" in b or "early" in b or "extraordinary" in b or "special" in b:
            return f"{body} (snap/extraordinary)"
        if "by-election" in b or "by_election" in b:
            return f"{body} (by-election)"
        return body or None

    return {
        "lastDate": max(past) if past else None,
        "nextDate": min(future) if future else None,
        "nextType": _eg_classify(next_record),
        "source": "electionguide",
    }


# ── REST COUNTRIES ────────────────────────────────────────────────────────────

def fetch_rest_countries(iso2: str) -> Dict[str, Any]:
    url = f"{REST_COUNTRIES_BASE}/alpha/{iso2.lower()}"
    data = req_json(url, label=f"REST Countries /alpha/{iso2}")

    if isinstance(data, list):
        data = data[0] if data else None
    elif not isinstance(data, dict):
        data = None

    if not data:
        return {
            "capital": None, "population": None, "region": None, "subregion": None,
            "flag": None, "flagPng": None, "currencies": [], "languages": [],
            "officialName": None, "source": "restcountries",
        }

    cap_raw = data.get("capital")
    capital = cap_raw[0] if isinstance(cap_raw, list) and cap_raw else None

    curr_raw = data.get("currencies") or {}
    currencies = [v["name"] for v in curr_raw.values()
                  if isinstance(v, dict) and v.get("name")]

    lang_raw = data.get("languages") or {}
    languages = list(lang_raw.values()) if isinstance(lang_raw, dict) else []

    name_obj = data.get("name") or {}
    official = name_obj.get("official") if isinstance(name_obj, dict) else None

    flags = data.get("flags") or {}
    flag_png = flags.get("png") if isinstance(flags, dict) else None

    return {
        "capital":      capital,
        "population":   data.get("population"),
        "region":       data.get("region"),
        "subregion":    data.get("subregion"),
        "flag":         data.get("flag"),
        "flagPng":      flag_png,
        "currencies":   currencies,
        "languages":    languages,
        "officialName": official,
        "source":       "restcountries",
    }


# ── WORLD BANK WGI ────────────────────────────────────────────────────────────

def _parse_wb(payload: Any) -> Tuple[Optional[float], Optional[int], Optional[str]]:
    rows = []
    if isinstance(payload, list):
        if len(payload) >= 2 and isinstance(payload[1], list):
            rows = payload[1]
        elif len(payload) >= 1 and isinstance(payload[0], list):
            rows = payload[0]
        else:
            rows = [x for x in payload if isinstance(x, dict) and "value" in x]
    elif isinstance(payload, dict):
        rows = payload.get("data") or payload.get("results") or []

    for row in rows:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        dt  = row.get("date")
        if val is None or dt is None:
            continue
        try:
            return float(val), int(str(dt)[:4]), None
        except Exception:
            continue
    return None, None, "No non-null value in WB series."

def fetch_wgi(iso2: str) -> Dict[str, Any]:
    # WGI source 3 requires uppercase ISO2 codes; overrides apply for Kosovo/Taiwan
    wb_code = WB_ISO2_OVERRIDES.get(iso2.upper(), iso2.upper())

    components: Dict[str, Any] = {}
    years:  List[int]   = []
    values: List[float] = []
    sources: Dict[str, str] = {}

    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        url = f"{WORLD_BANK_BASE}/country/{wb_code}/indicator/{code}"
        # source=3  -- Worldwide Governance Indicators (live, updated annually)
        # mrv=1     -- most recent value only (faster, less data to parse)
        payload = req_json(url, params={"source": "3", "format": "json", "mrv": 1},
                           label=f"WB {code} {iso2}")
        sources[dim] = url
        if payload is None:
            components[dim] = {"indicator": code, "percentile": None, "label": None,
                                "year": None, "notes": "Failed to fetch WB indicator."}
            continue
        v, y, notes = _parse_wb(payload)
        if v is not None and y is not None:
            components[dim] = {"indicator": code, "percentile": v,
                                "label": percentile_to_label(v, dim), "year": y}
            values.append(v)
            years.append(y)
        else:
            components[dim] = {"indicator": code, "percentile": None, "label": None,
                                "year": None, "notes": notes}

    if not values:
        return {"ok": False, "overallPercentile": None, "band": "unknown",
                "bandLabel": None, "year": None, "components": components,
                "sources": sources, "notes": "No WGI percentile values available."}

    overall = sum(values) / len(values)
    return {"ok": True, "overallPercentile": round(overall, 2),
            "band": percentile_to_tier(overall), "bandLabel": overall_label(overall),
            "year": max(years), "components": components, "sources": sources,
            "notes": None}

def merge_wb_sticky(new_wb: Dict, prev: Optional[Dict]) -> Dict:
    prev_wb = (prev or {}).get("worldBankGovernance") if isinstance(prev, dict) else None
    if new_wb.get("ok"):
        out = dict(new_wb)
        out.pop("ok", None)
        return out
    if isinstance(prev_wb, dict) and prev_wb.get("overallPercentile") is not None:
        kept = dict(prev_wb)
        kept["notes"] = f"Kept previous values; latest fetch failed: {new_wb.get('notes')}"
        return kept
    out = dict(new_wb)
    out.pop("ok", None)
    return out


# ── CLAUDE API ────────────────────────────────────────────────────────────────

ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages"
CLAUDE_MAX_TOKENS    = 4000
CLAUDE_FORCE_REFRESH = os.environ.get("CLAUDE_FORCE_REFRESH", "").strip() == "1"

# ── CLAUDE SYSTEM PROMPT ──────────────────────────────────────────────────────

CLAUDE_SYSTEM = """\
You are a political data analyst maintaining a structured JSON dataset of country \
leadership, legislature control, election data, and party profiles. You will receive:
  - The country name and ISO2 code
  - A sovereigntyNote if the country has special status (SAR, partially recognized, etc.)
  - Whatever the free scrapers found (Wikipedia leader names, IPU/EG election dates)
  - The previous snapshot for this country (may be months old — stripped to key fields only)
  - Today's date
  - Whether this is an election-watch call (electionWatchActive in previous snapshot)

Return ONLY a single valid JSON object — no markdown, no explanation — with these keys:

{
  "headOfState":          {"name": str, "partyOrGroup": str},
  "headOfGovernment":     {"name": str, "partyOrGroup": str},
  "politicalSystem":      [str, ...],
  "legislature":          [{"name": str, "inControl": str}, ...],
  "competitiveElections":       bool,
  "nonCompetitiveReason":       str | null,
  "electionsSuspended":         bool,
  "suspensionReason":           str | null,
  "lastCompetitivenessCheck":   str,
  "ipu_not_applicable":         bool,
  "ipu_not_applicable_reason":  str | null,
  "electionWatchActive":        bool,
  "electionWatchReason":        str | null,
  "legislative": {
    "lastElection": {"date": str, "type": str, "notes": str,
                     "runoffDate": str|null, "runoffCondition": str|null} | null,
    "nextElection": {"date": str, "type": str, "notes": str,
                     "runoffDate": str|null, "runoffCondition": str|null} | null
  },
  "executive": {
    "lastElection": {"date": str, "type": str, "notes": str,
                     "runoffDate": str|null, "runoffCondition": str|null} | null,
    "nextElection": {"date": str, "type": str, "notes": str,
                     "runoffDate": str|null, "runoffCondition": str|null} | null
  },
  "partyProfileUpdates": {
    "<party_name>": {
      "politicalOrientation": str,
      "ideologyTags": [str, ...],
      "keyPlatforms": [str, ...]
    }
  },
  "dataAvailabilityNotes": str | null
}

CRITICAL RULES:

1. NEVER put a future date in lastElection. lastElection must only contain elections
   where the vote has already been cast (date < today). If an election is scheduled
   but has not yet happened, it goes in nextElection only.

2. NEVER fabricate results. If uncertain whether an election has occurred, treat
   it as upcoming.

3. POST-ELECTION COALITION TALKS: notes must say "Coalition talks ongoing as of [date]".
   Do not state a PM has been confirmed until that is factual.

4. For acting/interim leaders, use the person actually exercising power today.

5. For one-party states: headOfState = formal president. Note supreme power-holder
   in dataAvailabilityNotes.

6. dataAvailabilityNotes is mandatory for: disputed legitimacy, parallel governments,
   suspended elections, acting leaders, one-party context, SAR status, partial
   sovereignty recognition, or any nuance needed.

7. election notes: factual, 1-2 sentences. Past-tense for completed events.

8. legislature[].inControl: reflect post-election reality. If coalition talks are
   ongoing, say "Pending coalition formation (election [date])".

9. runoffDate: only populate if a specific runoff date is scheduled.

10. WEB SEARCH IS MANDATORY ON EVERY CALL. You MUST use the web_search tool
    before producing your response. Minimum required searches:
      - Current head of state/government (always search)
      - Most recent election result (always search)
      - Any upcoming election within 6 months (always search)
      - For election-watch calls: "[country] election result [year]" and
        "[country] new government formed"
    Never produce output without first searching.

11. SOVEREIGNTY / SPECIAL STATUS: If a sovereigntyNote is provided, acknowledge
    the special status prominently in dataAvailabilityNotes.

12. ELECTION WATCH — electionWatchActive field:
    - TRUE if: election within 3 days, OR election occurred but no confirmed winner yet.
    - FALSE if: clear winner officially confirmed and power transferred.

13. partyProfileUpdates: ONLY include parties newly in power OR not yet profiled.
    The previousSnapshot.partyProfiles field lists existing party names — do NOT
    re-submit profiles already listed there.
    - politicalOrientation: one of "Far-Left", "Left", "Centre-Left", "Centre",
      "Centre-Right", "Right", "Far-Right", "Authoritarian", "Theocratic",
      "Nationalist", "Communist", "Mixed/Unclear"
    - ideologyTags: short list of tags
    - keyPlatforms: 3-5 bullet points

14. dataAvailabilityNotes MUST be plain text only — no HTML, no <cite> tags, no
    markdown, no angle brackets.

15. lastElection must NEVER be null for a competitive country that has held a
    national election within the last 10 years.

16. headOfGovernment.name must reflect the person ACTUALLY exercising executive
    power today — store the clean name only, no title prefixes.

17. For one-party states where General Secretary ≠ formal President:
      headOfState.name  = the formal President
      headOfGovernment  = the Premier/Prime Minister
      dataAvailabilityNotes must name the General Secretary as supreme power-holder.

18. lastCompetitivenessCheck: always set to today's ISO date (YYYY-MM-DD).

19. ipu_not_applicable: true for one-party states, absolute monarchies, countries
    with no functioning legislature/elections, civil war, or military junta.

20. Return ONLY the JSON object — no markdown fences, no preamble, no explanation.\
"""


# ── CLAUDE TRIGGER LOGIC ──────────────────────────────────────────────────────

def _clean_wiki(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    _TITLE_RE = re.compile(
        r"^(?:President|Prime\s+Minister|King|Queen|Emperor|Chancellor|"
        r"General\s+Secretary(?:\s+of\s+the\s+Communist\s+Party)?|"
        r"First\s+Secretary(?:\s+of\s+the\s+Communist\s+Party)?|"
        r"Premier|Governor[\s-]General|Grand\s+Duke)"
        r"(?:\s*\[\s*\w+\s*\])*\s*[\u2013\u2014-]\s*",
        re.IGNORECASE,
    )
    s = _TITLE_RE.sub("", s)
    s = re.sub(r"\s*\[\s*[^\]]*\]\s*", " ", s).strip()
    return s or None


def _days_since_claude(prev: Optional[Dict]) -> int:
    if not prev:
        return 999
    ts = prev.get("lastClaudeUpdate")
    if not ts:
        return 999
    try:
        last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last).days
    except (ValueError, AttributeError):
        return 999


def _snapshot_anomaly_detected(iso2: str, prev: Optional[Dict]) -> Tuple[bool, str]:
    if not prev:
        return False, ""

    alert = prev.get("changeInPowerAlert")
    if alert and not alert.get("resolved"):
        return True, "unresolved_sentinel_alert"

    if (prev.get("elections") or {}).get("electionWatchActive"):
        return True, "election_watch_active_in_snapshot"

    da = prev.get("dataAvailability") or {}
    for field_val in da.values():
        if isinstance(field_val, str) and "<cite" in field_val:
            return True, "cite_markup_in_dataAvailability"

    elections = prev.get("elections") or {}
    competitive = elections.get("competitiveElections")
    elections_suspended = elections.get("electionsSuspended", False)

    leg_last  = (elections.get("legislative") or {}).get("lastElection")
    exec_last = (elections.get("executive") or {}).get("lastElection")
    expect_election_data = (competitive is True and not elections_suspended)
    if expect_election_data and leg_last is None and exec_last is None:
        return True, "null_lastElection_for_competitive_country"

    exec_block = prev.get("executive") or {}
    hos_name = (exec_block.get("headOfState") or {}).get("name")
    hog_name = (exec_block.get("headOfGovernment") or {}).get("name")
    leg_block = prev.get("legislature") or {}
    leg_bodies = leg_block.get("bodies", [])
    pol_sys = (prev.get("politicalSystem") or {}).get("values", ["unknown"])
    data_is_blank = (
        not hos_name
        and not hog_name
        and not leg_bodies
        and pol_sys == ["unknown"]
    )
    if data_is_blank:
        if prev.get("claudeAttemptedWithNoData"):
            return False, ""
        return True, "blank_political_data"

    return False, ""


def _needs_competitiveness_refresh(prev: Optional[Dict], always_on: bool = False) -> Tuple[bool, str]:
    if not prev:
        return False, ""

    elections = prev.get("elections") or {}
    competitive = elections.get("competitiveElections")

    if competitive is None:
        return True, "competitiveness_never_assessed"

    last_check_str = elections.get("lastCompetitivenessCheck")
    if not last_check_str:
        return True, "competitiveness_check_date_missing"

    try:
        raw = last_check_str.strip().replace("Z", "+00:00")
        if len(raw) == 10:
            last_check = datetime(
                int(raw[:4]), int(raw[5:7]), int(raw[8:10]), tzinfo=timezone.utc
            )
        else:
            last_check = datetime.fromisoformat(raw)
            if last_check.tzinfo is None:
                last_check = last_check.replace(tzinfo=timezone.utc)
        days_since = (datetime.now(timezone.utc) - last_check).days
    except (ValueError, AttributeError, IndexError):
        return True, "competitiveness_check_date_unparseable"

    if days_since >= 365:
        return True, f"annual_competitiveness_refresh ({days_since}d since last check)"

    if not always_on:
        return False, ""

    suspended = elections.get("electionsSuspended", False)
    if (competitive is False or suspended) and days_since >= 180:
        return True, f"semi_annual_noncompetitive_refresh ({days_since}d since last check)"

    return False, ""


def _should_call_claude(
    iso2: str,
    wiki_names: Dict,
    ipu: Dict,
    eg: Dict,
    prev: Optional[Dict],
    biweekly_tuesday: bool,
    sentinel_alerts: Dict[str, str],
) -> Tuple[bool, str]:
    if CLAUDE_FORCE_REFRESH:
        return True, "forced_refresh"

    if not prev:
        if CLAUDE_FORCE_REFRESH or biweekly_tuesday:
            return True, "first_run"
        else:
            return False, "new_country_pending_biweekly"

    # ── Always-on triggers ─────────────────────────────────────────────────────
    watch_active, watch_reason = _election_watch_active(prev)
    if watch_active:
        return True, f"election_watch ({watch_reason})"

    if iso2.upper() in sentinel_alerts:
        return True, f"sentinel_alert: {sentinel_alerts[iso2.upper()]}"

    anomaly, anomaly_reason = _snapshot_anomaly_detected(iso2, prev)
    if anomaly:
        return True, f"snapshot_anomaly ({anomaly_reason})"

    needs_comp, comp_reason = _needs_competitiveness_refresh(prev, always_on=False)
    if needs_comp:
        return True, f"competitiveness_refresh ({comp_reason})"

    # ── Biweekly-only triggers ────────────────────────────────────────────────
    if not biweekly_tuesday:
        return False, ""

    needs_comp, comp_reason = _needs_competitiveness_refresh(prev, always_on=True)
    if needs_comp:
        return True, f"competitiveness_refresh ({comp_reason})"

    prev_hos = ((prev.get("executive") or {}).get("headOfState") or {}).get("name")
    prev_hog = ((prev.get("executive") or {}).get("headOfGovernment") or {}).get("name")
    wiki_hos = _clean_wiki(wiki_names.get("hosName"))
    wiki_hog = _clean_wiki(wiki_names.get("hogName"))
    if wiki_hos and wiki_hos != prev_hos:
        return True, f"executive_name_changed ({prev_hos!r} → {wiki_hos!r})"
    if wiki_hog and wiki_hog != prev_hog:
        return True, f"executive_name_changed ({prev_hog!r} → {wiki_hog!r})"

    prev_leg_next = ((prev.get("elections") or {}).get("legislative") or {}).get("nextElection") or {}
    ipu_next = ipu.get("nextDate")
    eg_next  = eg.get("nextDate")
    if ipu_next and ipu_next != prev_leg_next.get("date"):
        return True, f"ipu_date_changed ({ipu_next})"
    if eg_next and eg_next != prev_leg_next.get("date"):
        return True, f"eg_date_changed ({eg_next})"

    days_old = _days_since_claude(prev)
    if days_old >= 13:
        return True, f"biweekly_tuesday_refresh (last_update_{days_old}d_ago)"

    return False, ""


# ── CLAUDE API CALL ───────────────────────────────────────────────────────────

def _call_claude(
    country_name: str,
    iso2: str,
    wiki_names: Dict,
    ipu: Dict,
    eg: Dict,
    prev: Optional[Dict],
    trigger_reason: str,
    model: str,
) -> Optional[Dict]:
    """
    Call Claude with live web search. Handles the multi-turn tool-use loop.
    Uses the provided model (Sonnet for high-stakes, Haiku for low-stakes).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    today = datetime.now(timezone.utc).date().isoformat()

    election_watch_context = False
    if prev:
        election_watch_context = bool(
            (prev.get("elections") or {}).get("electionWatchActive")
        )
    watch_active, _ = _election_watch_active(prev)
    election_watch_context = election_watch_context or watch_active

    context = {
        "country": country_name,
        "iso2": iso2,
        "today": today,
        "triggerReason": trigger_reason,
        "electionWatchActive": election_watch_context,
        "sovereigntyNote": SOVEREIGNTY_NOTES.get(iso2.upper()),
        "scraperData": {
            "wikipedia": {
                "hosName": _clean_wiki(wiki_names.get("hosName")),
                "hogName": _clean_wiki(wiki_names.get("hogName")),
            },
            "ipu": {
                "lastDate": ipu.get("lastDate"),
                "nextDate": ipu.get("nextDate"),
                "nextType": ipu.get("nextType"),
            },
            "electionGuide": {
                "lastDate": eg.get("lastDate"),
                "nextDate": eg.get("nextDate"),
                "nextType": eg.get("nextType"),
            },
        },
        # Slim context — only fields Claude needs, not the full snapshot blob
        "previousSnapshot": _slim_prev(prev),
    }

    WEB_SEARCH_TOOL = {
        "type": "web_search_20250305",
        "name": "web_search",
    }

    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }

    messages = [{"role": "user", "content": json.dumps(context, ensure_ascii=False)}]

    try:
        max_turns = 8
        final_text = ""

        for turn in range(max_turns):
            resp = requests.post(
                ANTHROPIC_API_URL,
                headers=headers,
                json={
                    "model":      model,
                    "max_tokens": CLAUDE_MAX_TOKENS,
                    "system":     CLAUDE_SYSTEM,
                    "tools":      [WEB_SEARCH_TOOL],
                    "messages":   messages,
                },
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()

            stop_reason = data.get("stop_reason")
            content_blocks = data.get("content", [])

            for block in content_blocks:
                if block.get("type") == "text":
                    final_text += block.get("text", "")

            if stop_reason == "end_turn":
                break

            if stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": content_blocks})

                tool_results = []
                for block in content_blocks:
                    if block.get("type") == "tool_use":
                        tool_id    = block.get("id", "")
                        tool_name  = block.get("name", "")
                        tool_input = block.get("input", {})

                        if tool_name == "web_search":
                            query = tool_input.get("query", "")
                            print(f"  [{iso2}] 🔍 Web search: {query!r}")

                        tool_results.append({
                            "type":        "tool_result",
                            "tool_use_id": tool_id,
                            "content":     block.get("content", ""),
                        })

                messages.append({"role": "user", "content": tool_results})
                continue

            print(f"  [{iso2}] ⚠️  Unexpected stop_reason: {stop_reason}")
            break

        if not final_text.strip():
            print(f"  [{iso2}] ⚠️  Claude returned no text after {turn + 1} turn(s)")
            return None

        raw = final_text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        brace_start = raw.find("{")
        brace_end   = raw.rfind("}") + 1
        if brace_start != -1 and brace_end > brace_start:
            raw = raw[brace_start:brace_end]

        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError(f"Expected dict, got {type(result)}")
        return result

    except json.JSONDecodeError as e:
        print(f"  [{iso2}] ⚠️  Claude JSON parse error: {e}. Raw: {final_text[:300]}")
    except requests.HTTPError as e:
        print(f"  [{iso2}] ⚠️  Claude HTTP error: {e}")
    except Exception as e:
        print(f"  [{iso2}] ⚠️  Claude error: {e}")
    return None


# ── PARTY PROFILES ────────────────────────────────────────────────────────────

def _merge_party_profiles(
    prev_profiles: Optional[Dict],
    new_updates: Optional[Dict],
) -> Optional[Dict]:
    result = dict(prev_profiles or {})
    if new_updates and isinstance(new_updates, dict):
        for party_name, profile in new_updates.items():
            if party_name and isinstance(profile, dict):
                result[party_name] = {
                    "politicalOrientation": profile.get("politicalOrientation"),
                    "ideologyTags":         profile.get("ideologyTags", []),
                    "keyPlatforms":         profile.get("keyPlatforms", []),
                    "lastUpdated":          iso_z(now_utc()),
                }
    return result if result else None


# ── ASSEMBLE FROM CLAUDE ──────────────────────────────────────────────────────

def _assemble_from_claude(
    iso2: str, cl: Dict, ipu: Dict, eg: Dict,
    trigger: str, today_str: str,
    prev_profiles: Optional[Dict],
) -> Tuple[Dict, Dict, Dict, Optional[Dict]]:

    def _norm_election(obj: Optional[Dict]) -> Optional[Dict]:
        if not obj:
            return None
        return {
            "date":            obj.get("date"),
            "type":            obj.get("type"),
            "notes":           obj.get("notes"),
            "runoffDate":      obj.get("runoffDate"),
            "runoffCondition": obj.get("runoffCondition"),
            "electionDay":     str(obj.get("date", "")) == today_str,
        }

    def _flag_today(obj: Optional[Dict]) -> Optional[Dict]:
        if not obj or str(obj.get("date", "")) != today_str:
            return obj
        obj = dict(obj)
        obj["electionDay"] = True
        existing = obj.get("notes") or ""
        obj["notes"] = (f"⚡ ELECTION DAY ({today_str}). " + existing).strip()
        return obj

    hos = cl.get("headOfState") or {}
    hog = cl.get("headOfGovernment") or {}
    hos_name = hos.get("name")
    hog_name = hog.get("name")
    exec_leader = hog_name or hos_name
    exec_party  = hog.get("partyOrGroup") or hos.get("partyOrGroup")

    executive_block = {
        "headOfState": {
            "name":         hos_name,
            "partyOrGroup": hos.get("partyOrGroup"),
            "source":       f"claude ({trigger})",
        },
        "headOfGovernment": {
            "name":         hog_name,
            "partyOrGroup": hog.get("partyOrGroup"),
            "source":       f"claude ({trigger})",
        },
        "executiveInPower": {
            "leader":       exec_leader,
            "partyOrGroup": exec_party,
            "method":       "head_of_government" if hog_name and hog_name != hos_name
                            else "head_of_state",
        },
    }

    leg_bodies = cl.get("legislature") or []
    legislature_block = {
        "bodies": [
            {
                "name":          b.get("name", "Legislature"),
                "inControl":     b.get("inControl", "unknown"),
                "controlMethod": f"claude ({trigger})",
            }
            for b in leg_bodies
        ],
        "source": f"claude ({trigger})",
    }

    cl_leg  = cl.get("legislative") or {}
    cl_exec = cl.get("executive")   or {}

    leg_next  = _flag_today(_norm_election(cl_leg.get("nextElection")))
    exec_next = _flag_today(_norm_election(cl_exec.get("nextElection")))

    election_today = (
        str((cl_leg.get("nextElection") or {}).get("date", "")) == today_str or
        str((cl_exec.get("nextElection") or {}).get("date", "")) == today_str
    )

    def _maybe_refine_date(election_obj: Optional[Dict], scraper_date: Optional[str]) -> Optional[Dict]:
        if not election_obj or not scraper_date or len(scraper_date) != 10:
            return election_obj
        static_year = str((election_obj.get("date") or ""))[:4]
        if scraper_date.startswith(static_year):
            election_obj = dict(election_obj)
            election_obj["date"] = scraper_date
        return election_obj

    leg_next = _maybe_refine_date(leg_next, ipu.get("nextDate") or eg.get("nextDate"))

    election_watch_active = bool(cl.get("electionWatchActive", False))
    election_watch_reason = cl.get("electionWatchReason")
    today_iso = today_str

    elections_block = {
        "competitiveElections":      cl.get("competitiveElections", True),
        "nonCompetitiveReason":      cl.get("nonCompetitiveReason"),
        "electionsSuspended":        cl.get("electionsSuspended", False),
        "suspensionReason":          cl.get("suspensionReason"),
        "lastCompetitivenessCheck":  cl.get("lastCompetitivenessCheck", today_iso),
        "ipu_not_applicable":        bool(cl.get("ipu_not_applicable", False)),
        "ipu_not_applicable_reason": cl.get("ipu_not_applicable_reason"),
        "electionToday":             election_today,
        "electionWatchActive":       election_watch_active,
        "electionWatchReason":       election_watch_reason,
        "legislative": {
            "lastElection": _norm_election(cl_leg.get("lastElection")),
            "nextElection": leg_next,
            "source": f"claude ({trigger})" +
                      (" + ipu_parline" if ipu.get("nextDate") else "") +
                      (" + electionguide" if eg.get("nextDate") else ""),
        },
        "executive": {
            "lastElection": _norm_election(cl_exec.get("lastElection")),
            "nextElection": exec_next,
            "source":       f"claude ({trigger})",
        },
    }

    party_profiles = _merge_party_profiles(prev_profiles, cl.get("partyProfileUpdates"))

    return executive_block, legislature_block, elections_block, party_profiles


# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def build_country(
    name: str,
    iso2: str,
    prev_by_iso2: Dict[str, Any],
    biweekly_tuesday: bool,
    sentinel_alerts: Dict[str, str],
    claude_calls_made: List[int],   # mutable counter: [current_count]
) -> Tuple[Dict[str, Any], bool]:
    prev = prev_by_iso2.get(iso2)
    today_str = datetime.now(timezone.utc).date().isoformat()

    # ── Free scrapers ──────────────────────────────────────────────────────────
    print(f"  [{iso2}] Wikipedia lookup...")
    wiki = _load_wiki_exec_cache().get(iso2, {})
    print(f"  [{iso2}] HOS={_clean_wiki(wiki.get('hosName'))}, HOG={_clean_wiki(wiki.get('hogName'))}")

    print(f"  [{iso2}] IPU elections fetch...")
    ipu = fetch_ipu_elections(iso2, prev)
    print(f"  [{iso2}] IPU: last={ipu.get('lastDate')} next={ipu.get('nextDate')} src={ipu.get('source')}")

    print(f"  [{iso2}] ElectionGuide lookup...")
    eg = get_electionguide_dates(iso2)
    print(f"  [{iso2}] EG: last={eg.get('lastDate')} next={eg.get('nextDate')}")

    print(f"  [{iso2}] World Bank WGI fetch...")
    wb_gov = merge_wb_sticky(fetch_wgi(iso2), prev)

    print(f"  [{iso2}] REST Countries fetch...")
    meta = fetch_rest_countries(iso2)

    # ── Claude trigger decision ────────────────────────────────────────────────
    should_call, trigger_reason = _should_call_claude(
        iso2, wiki, ipu, eg, prev, biweekly_tuesday, sentinel_alerts,
    )

    cl = None
    claude_attempted_no_data = False
    deferred = False

    if should_call:
        priority = _trigger_priority(trigger_reason)
        use_haiku = _use_haiku(trigger_reason)
        model = CLAUDE_MODEL_HAIKU if use_haiku else CLAUDE_MODEL_SONNET
        model_label = "Haiku" if use_haiku else "Sonnet"

        # Enforce the per-run cap — Priority 1 is never deferred
        if priority > 1 and claude_calls_made[0] >= MAX_CLAUDE_CALLS_PER_RUN:
            print(f"  [{iso2}] ⏸  DEFERRED — cap of {MAX_CLAUDE_CALLS_PER_RUN} Claude calls reached "
                  f"(priority {priority}, trigger: {trigger_reason})")
            deferred = True
            should_call = False
        else:
            print(f"  [{iso2}] 🤖 HARD RUN [{model_label}] — Claude triggered: {trigger_reason}")
            cl = _call_claude(name, iso2, wiki, ipu, eg, prev, trigger_reason, model)
            claude_calls_made[0] += 1

            # Adaptive sleep after the call
            sleep_secs = CLAUDE_SLEEP_HAIKU_SECONDS if use_haiku else CLAUDE_SLEEP_SECONDS
            print(f"  [{iso2}] 💤 Sleeping {sleep_secs}s after Claude call "
                  f"({claude_calls_made[0]}/{MAX_CLAUDE_CALLS_PER_RUN} calls used)")
            time.sleep(sleep_secs)
    else:
        days_old = _days_since_claude(prev)
        print(f"  [{iso2}] 💤 SOFT RUN — carrying forward data (last Claude: {days_old}d ago)")

    if should_call and not cl:
        claude_attempted_no_data = True
    elif should_call and cl:
        hos = (cl.get("headOfState") or {}).get("name")
        hog = (cl.get("headOfGovernment") or {}).get("name")
        if not hos and not hog:
            claude_attempted_no_data = True

    # ── Assemble output ────────────────────────────────────────────────────────
    prev_profiles = (prev or {}).get("partyProfiles")

    if cl:
        executive_block, legislature_block, elections_block, party_profiles = \
            _assemble_from_claude(iso2, cl, ipu, eg, trigger_reason, today_str, prev_profiles)
        pol_sys = {"values": cl.get("politicalSystem", ["unknown"]),
                   "source": f"claude ({trigger_reason})"}
        data_avail_note = cl.get("dataAvailabilityNotes")
        last_claude_update = iso_z(now_utc())
    elif prev:
        print(f"  [{iso2}] ↩  Carrying forward previous political data")
        executive_block   = prev.get("executive",   {})
        legislature_block = prev.get("legislature", {})
        elections_block   = prev.get("elections",   {})
        party_profiles    = prev_profiles
        pol_sys           = prev.get("politicalSystem", {"values": ["unknown"], "source": "carried_forward"})
        data_avail_note   = (prev.get("dataAvailability") or {}).get("executive")
        last_claude_update = prev.get("lastClaudeUpdate")
    else:
        print(f"  [{iso2}] ⚠️  No Claude response and no previous data — output will be sparse")
        wiki_hos = _clean_wiki(wiki.get("hosName"))
        wiki_hog = _clean_wiki(wiki.get("hogName"))
        executive_block = {
            "headOfState":      {"name": wiki_hos, "partyOrGroup": None, "source": "wikipedia"},
            "headOfGovernment": {"name": wiki_hog, "partyOrGroup": None, "source": "wikipedia"},
            "executiveInPower": {"leader": wiki_hog or wiki_hos, "partyOrGroup": None,
                                 "method": "head_of_government" if wiki_hog else "head_of_state"},
        }
        legislature_block = {"bodies": [], "source": "unknown"}
        elections_block   = {
            "competitiveElections":      None,  "nonCompetitiveReason":      None,
            "electionsSuspended":        False, "suspensionReason":           None,
            "lastCompetitivenessCheck":  None,
            "ipu_not_applicable":        False, "ipu_not_applicable_reason":  None,
            "electionToday":             False,
            "electionWatchActive":       False, "electionWatchReason":        None,
            "legislative": {"lastElection": None, "nextElection": None, "source": "unknown"},
            "executive":   {"lastElection": None, "nextElection": None, "source": "unknown"},
        }
        party_profiles    = None
        pol_sys           = {"values": ["unknown"], "source": "unknown"}
        data_avail_note   = None
        last_claude_update = None

    sentinel_alert = sentinel_alerts.get(iso2.upper())

    avail: Dict[str, str] = {}
    if data_avail_note:
        avail["executive"] = data_avail_note
    if wb_gov.get("overallPercentile") is None:
        avail["worldBankGovernance"] = f"World Bank WGI data unavailable for '{iso2}'."
    if not meta.get("capital") and not meta.get("population"):
        avail["metadata"] = f"REST Countries API returned no data for '{iso2}'."

    entry = {
        "country":  name,
        "iso2":     iso2,
        "metadata": {
            "officialName": meta["officialName"],
            "capital":      meta["capital"],
            "population":   meta["population"],
            "region":       meta["region"],
            "subregion":    meta["subregion"],
            "flag":         meta["flag"],
            "flagPng":      meta["flagPng"],
            "currencies":   meta["currencies"],
            "languages":    meta["languages"],
            "source":       meta["source"],
        },
        "politicalSystem":     pol_sys,
        "executive":           executive_block,
        "legislature":         legislature_block,
        "partyProfiles":       party_profiles,
        "worldBankGovernance": wb_gov,
        "dataAvailability":    avail if avail else None,
        "elections":           elections_block,
        "lastClaudeUpdate":          last_claude_update,
        "claudeAttemptedWithNoData": claude_attempted_no_data,
        "claudeDeferred":            deferred,
    }

    if sentinel_alert:
        entry["changeInPowerAlert"] = {
            "alert":      sentinel_alert,
            "detectedAt": iso_z(now_utc()),
            "resolved":   False,
        }
    elif prev and prev.get("changeInPowerAlert") and not prev["changeInPowerAlert"].get("resolved"):
        if cl:
            entry["changeInPowerAlert"] = dict(prev["changeInPowerAlert"])
            entry["changeInPowerAlert"]["resolved"] = True
            entry["changeInPowerAlert"]["resolvedAt"] = iso_z(now_utc())
        else:
            entry["changeInPowerAlert"] = prev["changeInPowerAlert"]

    used_claude = cl is not None
    return entry, used_claude


# ── PRE-SCAN: DETERMINE CALL PLAN ─────────────────────────────────────────────

def _plan_calls(
    prev_by_iso2: Dict[str, Any],
    biweekly_tuesday: bool,
    sentinel_alerts: Dict[str, str],
) -> None:
    """
    Print a summary of how many Claude calls are planned and at what priority,
    so the operator can see whether deferral will happen before the run starts.
    """
    p1, p2, p3 = [], [], []
    soft = []

    wiki_cache = _load_wiki_exec_cache()

    for c in COUNTRIES:
        iso2 = c["iso2"]
        prev = prev_by_iso2.get(iso2)
        wiki = wiki_cache.get(iso2, {})
        ipu  = {"lastDate": None, "nextDate": None, "nextType": None}
        eg   = {"lastDate": None, "nextDate": None, "nextType": None}

        should_call, reason = _should_call_claude(
            iso2, wiki, ipu, eg, prev, biweekly_tuesday, sentinel_alerts
        )
        if not should_call:
            soft.append(iso2)
            continue

        pri = _trigger_priority(reason)
        if pri == 1:
            p1.append((iso2, reason))
        elif pri == 2:
            p2.append((iso2, reason))
        else:
            p3.append((iso2, reason))

    total_hard = len(p1) + len(p2) + len(p3)
    will_defer = max(0, total_hard - MAX_CLAUDE_CALLS_PER_RUN)
    # Priority 1 is never deferred; deferral comes from p3 first, then p2
    defer_from_p3 = min(will_defer, len(p3))
    defer_from_p2 = max(0, will_defer - defer_from_p3)

    print(f"\n── Call Plan ─────────────────────────────────────────────────────────")
    print(f"  Soft runs (no Claude):        {len(soft)}")
    print(f"  Hard runs planned:            {total_hard}")
    print(f"    Priority 1 (never deferred): {len(p1)}  [{', '.join(i for i,_ in p1)}]")
    print(f"    Priority 2:                  {len(p2)}")
    print(f"    Priority 3 (Haiku):          {len(p3)}")
    print(f"  Cap:                          {MAX_CLAUDE_CALLS_PER_RUN}")
    print(f"  Will execute:                 {min(total_hard, MAX_CLAUDE_CALLS_PER_RUN)}")
    print(f"  Will defer:                   {will_defer}")
    if will_defer:
        print(f"    (deferred p3: {defer_from_p3}, deferred p2: {defer_from_p2})")
    print()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("docs") / "countries_snapshot.json"

    prev_full = load_full_previous_snapshot(out_path)
    prev_by_iso2 = {c["iso2"]: c for c in prev_full.get("countries", []) if c.get("iso2")}
    print(f"=== Starting build. Previous snapshot: {len(prev_by_iso2)} countries cached ===")
    print(f"  Rate-limit config: cap={MAX_CLAUDE_CALLS_PER_RUN}, "
          f"sleep_sonnet={CLAUDE_SLEEP_SECONDS}s, sleep_haiku={CLAUDE_SLEEP_HAIKU_SECONDS}s")

    biweekly_tuesday = _is_biweekly_tuesday(prev_full)
    if CLAUDE_FORCE_REFRESH:
        print(f"  [SCHEDULE] 🔴 FORCED REFRESH — all countries will get a hard Claude pull")
    elif biweekly_tuesday:
        print(f"  [SCHEDULE] 📅 BIWEEKLY TUESDAY — full sweep triggered")
    else:
        today_wd = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][datetime.now(timezone.utc).weekday()]
        days_since_sweep = "?"
        last_sweep = prev_full.get("lastFullSweepDate")
        if last_sweep:
            try:
                last_dt = datetime.fromisoformat(last_sweep.replace("Z", "+00:00"))
                days_since_sweep = (datetime.now(timezone.utc) - last_dt).days
            except Exception:
                pass
        print(f"  [SCHEDULE] 💤 SOFT DAILY RUN — {today_wd}, {days_since_sweep}d since last sweep")

    anomaly_countries: List[str] = []
    for c in COUNTRIES:
        p = prev_by_iso2.get(c["iso2"])
        has_anomaly, _ = _snapshot_anomaly_detected(c["iso2"], p)
        if has_anomaly:
            anomaly_countries.append(c["iso2"])
    if anomaly_countries:
        print(f"  [ANOMALY]  ⚠️  Snapshot anomalies: {', '.join(anomaly_countries)}")

    sentinel_alerts = run_change_in_power_sentinel(prev_full)

    sentinel_feed_raw = req_json(CHANGE_IN_POWER_URL, label="sentinel feed (id update)")
    sentinel_articles: List[Dict] = []
    if isinstance(sentinel_feed_raw, list):
        sentinel_articles = sentinel_feed_raw
    elif isinstance(sentinel_feed_raw, dict):
        sentinel_articles = (
            sentinel_feed_raw.get("articles") or
            sentinel_feed_raw.get("items") or
            sentinel_feed_raw.get("data") or []
        )
    updated_seen_ids = update_sentinel_seen_ids(prev_full, sentinel_articles)

    _load_ipu_parliament_map()
    _load_electionguide_cache()
    _load_wiki_exec_cache()

    # Print call plan summary before processing starts
    _plan_calls(prev_by_iso2, biweekly_tuesday, sentinel_alerts)

    out = {
        "generatedAt":        iso_z(now_utc()),
        "lastFullSweepDate":  (
            iso_z(now_utc()) if biweekly_tuesday
            else prev_full.get("lastFullSweepDate", iso_z(now_utc()))
        ),
        "worldBankYearRule":  "latest_non_null_per_indicator",
        "sentinelSeenIds":    updated_seen_ids,
        "countries":          [],
        "sources": {
            "executives":             "wikipedia_adaptive + claude_api (smart diff, biweekly ceiling)",
            "legislature":            "claude_api (smart diff, biweekly ceiling)",
            "elections":              "ipu_parline + electionguide + claude_api (daily near elections)",
            "changeInPowerSentinel":  CHANGE_IN_POWER_URL,
            "wikipedia_adaptive":     WIKIPEDIA_API,
            "world_bank_base":        WORLD_BANK_BASE,
            "ipu_parline":            f"{IPU_API_BASE}/api",
            "electionguide":          ELECTIONGUIDE_BASE,
            "rest_countries":         REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
        "electionDataModel": {
            "description": (
                "Each country's elections block contains competitiveElections (bool), "
                "nonCompetitiveReason (str|null), electionWatchActive (bool — true when "
                "an election is within 3 days or results are still pending), and "
                "legislative/executive sub-blocks each with lastElection and nextElection. "
                "Election objects contain: date, type, notes, runoffDate, runoffCondition. "
                "partyProfiles contains orientation, ideology tags, and key platforms for "
                "each party currently or recently in power."
            ),
        },
        "rateLimitConfig": {
            "maxClaudeCallsPerRun": MAX_CLAUDE_CALLS_PER_RUN,
            "sleepSecondsSonnet":   CLAUDE_SLEEP_SECONDS,
            "sleepSecondsHaiku":    CLAUDE_SLEEP_HAIKU_SECONDS,
            "modelSonnet":          CLAUDE_MODEL_SONNET,
            "modelHaiku":           CLAUDE_MODEL_HAIKU,
        },
    }

    # Shared mutable counter — passed into build_country so it can enforce the cap
    claude_calls_made = [0]

    for c in COUNTRIES:
        print(f"\n▶ {c['country']} ({c['iso2']})")
        country_data, used_claude = build_country(
            c["country"], c["iso2"], prev_by_iso2,
            biweekly_tuesday, sentinel_alerts,
            claude_calls_made,
        )
        out["countries"].append(country_data)
        # No extra sleep here — adaptive sleep is now inside build_country after each call

    print(f"\n✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")
    print(f"   Total Claude calls this run: {claude_calls_made[0]} / {MAX_CLAUDE_CALLS_PER_RUN}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
