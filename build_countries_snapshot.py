"""
Build a Base44-friendly JSON snapshot for multiple countries.
Output: docs/countries_snapshot.json

Run:  python scripts/build_countries_snapshot.py
Deps: pip install requests beautifulsoup4 lxml

Data strategy (March 2026):
  - Executive names/parties:    Wikipedia (free) → Claude API (fills gaps, verifies)
  - Legislature bodies/control: Claude API (smart diff, 7-day refresh ceiling)
  - Elections:                  IPU Parline + ElectionGuide (dates) → Claude API (context, notes)
                                Daily refresh triggered when election within 7 days
  - Political system:           Claude API
  - Metadata:                   REST Countries API (live)
  - Governance:                 World Bank WGI API (live)

Election data model per country:
  elections:
    competitiveElections: bool
    nonCompetitiveReason: str | null
    legislative:
      lastElection:  { date, type, notes }  | null
      nextElection:  { date, type, notes }  | null
      source: str
    executive:
      lastElection:  { date, type, notes }  | null
      nextElection:  { date, type, notes }  | null
      source: str
"""

from __future__ import annotations

import json
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
# IPU Parline v2 API — correct base and endpoints as of 2025
# Docs: https://data.ipu.org/api-doc
IPU_API_BASE         = "https://data.ipu.org"
IPU_PARLIAMENTS_URL  = f"{IPU_API_BASE}/api/parliaments"
IPU_ELECTIONS_URL    = f"{IPU_API_BASE}/api/elections"
REST_COUNTRIES_BASE  = "https://restcountries.com/v3.1"
WIKIPEDIA_API        = "https://en.wikipedia.org/w/api.php"
ELECTIONGUIDE_BASE   = "https://electionguide.org"

WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability":      "VA.PER.RNK",
    "politicalStability":       "PV.PER.RNK",
    "governmentEffectiveness":  "GE.PER.RNK",
    "regulatoryQuality":        "RQ.PER.RNK",
    "ruleOfLaw":                "RL.PER.RNK",
    "controlOfCorruption":      "CC.PER.RNK",
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

# ── COUNTRY LIST ──────────────────────────────────────────────────────────────

COUNTRIES: List[Dict[str, str]] = [
    {"country": "Russia",         "iso2": "RU"},
    {"country": "India",          "iso2": "IN"},
    {"country": "Pakistan",       "iso2": "PK"},
    {"country": "China",          "iso2": "CN"},
    {"country": "United Kingdom", "iso2": "GB"},
    {"country": "Germany",        "iso2": "DE"},
    {"country": "UAE",            "iso2": "AE"},
    {"country": "Saudi Arabia",   "iso2": "SA"},
    {"country": "Israel",         "iso2": "IL"},
    {"country": "Palestine",      "iso2": "PS"},
    {"country": "Mexico",         "iso2": "MX"},
    {"country": "Brazil",         "iso2": "BR"},
    {"country": "Canada",         "iso2": "CA"},
    {"country": "Nigeria",        "iso2": "NG"},
    {"country": "Japan",          "iso2": "JP"},
    {"country": "Iran",           "iso2": "IR"},
    {"country": "Syria",          "iso2": "SY"},
    {"country": "France",         "iso2": "FR"},
    {"country": "Turkey",         "iso2": "TR"},
    {"country": "Venezuela",      "iso2": "VE"},
    {"country": "Vietnam",        "iso2": "VN"},
    {"country": "Taiwan",         "iso2": "TW"},
    {"country": "South Korea",    "iso2": "KR"},
    {"country": "North Korea",    "iso2": "KP"},
    {"country": "Indonesia",      "iso2": "ID"},
    {"country": "Myanmar",        "iso2": "MM"},
    {"country": "Armenia",        "iso2": "AM"},
    {"country": "Azerbaijan",     "iso2": "AZ"},
    {"country": "Morocco",        "iso2": "MA"},
    {"country": "Somalia",        "iso2": "SO"},
    {"country": "Yemen",          "iso2": "YE"},
    {"country": "Libya",          "iso2": "LY"},
    {"country": "Egypt",          "iso2": "EG"},
    {"country": "Algeria",        "iso2": "DZ"},
    {"country": "Argentina",      "iso2": "AR"},
    {"country": "Chile",          "iso2": "CL"},
    {"country": "Peru",           "iso2": "PE"},
    {"country": "Cuba",           "iso2": "CU"},
    {"country": "Colombia",       "iso2": "CO"},
    {"country": "Panama",         "iso2": "PA"},
    {"country": "El Salvador",    "iso2": "SV"},
    {"country": "Denmark",        "iso2": "DK"},
    {"country": "Sudan",          "iso2": "SD"},
    {"country": "Ukraine",        "iso2": "UA"},
]

# Countries where elections are not competitive / not meaningful to track.
# Value is the human-readable reason shown in output.
NON_COMPETITIVE: Dict[str, str] = {
    "CN": "One-party state. The Chinese Communist Party holds a monopoly on political power. National People's Congress 'elections' are uncontested single-party votes.",
    "KP": "Totalitarian single-party state. Supreme People's Assembly elections feature a single Korean Workers' Party-approved candidate per seat with near-100% reported turnout.",
    "CU": "One-party socialist republic. The Communist Party of Cuba is the only legal party. National Assembly candidates are pre-approved.",
    "VN": "One-party socialist republic. The Communist Party of Vietnam controls all state institutions. National Assembly candidates are vetted by the party.",
    "SA": "Absolute monarchy with no national elections for executive or legislative positions. The Consultative Assembly (Majlis al-Shura) is fully appointed by royal decree.",
    "AE": "Federal constitutional monarchy. Executive positions are hereditary. The Federal National Council is half-appointed, half indirectly elected via a limited electorate.",
    "SY": "Transitional authority following Assad's fall (December 2024). No elections scheduled; country is governed by interim administration (Hayat Tahrir al-Sham).",
    "SD": "Military junta. Sudan is under Sudanese Armed Forces (SAF) control amid civil war with the RSF. No elections scheduled or possible under current conditions.",
    "YE": "Civil war. Two parallel governments (Houthi/Ansar Allah in the north; Presidential Leadership Council in the south). No elections possible.",
    "LY": "Divided state with two rival governments. Elections scheduled for 2021 were never held. No current electoral process.",
    "MM": "Military junta (State Administration Council) since the February 2021 coup. The elected NLD government operates in exile as the National Unity Government.",
    "PS": "No elections held since 2006 (legislative) and 2005 (presidential). Mahmoud Abbas rules by decree; Hamas controls Gaza. Elections indefinitely postponed.",
}

# Countries where IPU data is not applicable (not IPU members or not tracked)
IPU_NOT_APPLICABLE: Dict[str, str] = {
    "TW": "Taiwan is not an IPU member (non-UN member state).",
    "KP": "North Korea holds nominal single-party elections not tracked by IPU as competitive.",
    "CN": "Non-competitive. Not meaningfully tracked by IPU.",
    "CU": "Non-competitive single-party state.",
    "VN": "Non-competitive single-party state.",
    "SA": "No elections. Not in IPU.",
    "AE": "No national elections. Not in IPU.",
    "SY": "No elections. Transitional authority.",
    "SD": "No elections. Military junta.",
    "YE": "No elections. Civil war.",
    "LY": "No elections. Divided state.",
    "MM": "No elections. Military coup.",
    "PS": "No elections. Legislative Council suspended.",
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
    """Fetch a URL and return the raw HTML text."""
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
        data = json.loads(path.read_text(encoding="utf-8"))
        return {c["iso2"]: c for c in data.get("countries", []) if c.get("iso2")}
    except Exception:
        return {}

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
        "SA": "Saudi Arabia", "IL": "Israel", "PS": "Palestine",
        "MX": "Mexico", "BR": "Brazil", "CA": "Canada", "NG": "Nigeria",
        "JP": "Japan", "IR": "Iran", "SY": "Syria", "FR": "France",
        "TR": "Turkey", "VE": "Venezuela", "VN": "Vietnam",
        "KR": "South Korea", "KP": "North Korea", "ID": "Indonesia",
        "MM": "Myanmar", "AM": "Armenia", "AZ": "Azerbaijan", "MA": "Morocco",
        "SO": "Somalia", "YE": "Yemen", "LY": "Libya", "EG": "Egypt",
        "DZ": "Algeria", "AR": "Argentina", "CL": "Chile", "PE": "Peru",
        "CU": "Cuba", "CO": "Colombia", "PA": "Panama", "SV": "El Salvador",
        "DK": "Denmark", "SD": "Sudan", "UA": "Ukraine",
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

# ── IPU PARLINE (correct v2 API) ──────────────────────────────────────────────
# IPU Parline API documentation: https://data.ipu.org/api-doc
# The correct approach is:
#   GET /api/parliaments          → list all parliaments with their IDs
#   GET /api/elections?...        → filter elections by parliament ID
# The old /v1/chambers/{ISO2} endpoint no longer exists.

_ipu_parliament_map: Optional[Dict[str, Dict]] = None  # iso2 → parliament record

def _load_ipu_parliament_map() -> Dict[str, Dict]:
    """
    Load the full IPU parliament list once and build a lookup by ISO2.
    IPU uses its own parliament IDs, so we need this map to query elections.
    """
    global _ipu_parliament_map
    if _ipu_parliament_map is not None:
        return _ipu_parliament_map

    print("  [IPU] Loading parliament list from IPU Parline API...")
    _ipu_parliament_map = {}

    # Try the v2 parliaments endpoint
    # IPU API returns paginated JSON; iterate pages
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

        # Handle both list and paginated dict response
        records: List[Dict] = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data") or data.get("results") or data.get("parliaments") or []
            # Check if this is the only page
            if not records and "id" in data:
                records = [data]

        if not records:
            print(f"  [IPU] No records on page {page}, stopping pagination")
            break

        for rec in records:
            if not isinstance(rec, dict):
                continue
            # Extract ISO2 from various possible fields
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

        # If fewer records than per_page, we've hit the last page
        if len(records) < per_page:
            break

        page += 1
        time.sleep(0.3)

    print(f"  [IPU] Parliament map loaded: {len(_ipu_parliament_map)} countries")

    # Log a sample to understand the data shape
    if _ipu_parliament_map:
        sample_iso2 = next(iter(_ipu_parliament_map))
        sample = _ipu_parliament_map[sample_iso2]
        print(f"  [IPU] Sample record ({sample_iso2}) keys: {list(sample.keys())[:15]}")

    return _ipu_parliament_map


def _get_ipu_elections_for_country(iso2: str) -> List[Dict]:
    """
    Fetch recent and upcoming elections for a country from IPU.
    Returns a list of election records sorted by date descending.
    """
    parl_map = _load_ipu_parliament_map()
    parl = parl_map.get(iso2.upper())

    if not parl:
        return []

    # Try to get parliament ID in various formats
    parl_id = parl.get("id") or parl.get("parliamentId") or parl.get("parliament_id")
    if not parl_id:
        return []

    print(f"    [IPU] Fetching elections for {iso2} (parliament ID: {parl_id})")

    # Query elections endpoint
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
    """Try multiple field names for the election date in an IPU record."""
    for field in ("date", "electionDate", "election_date", "dateOfElection",
                  "lastElectionDate", "date_of_election"):
        val = rec.get(field)
        if val:
            d = _parse_ipu_date(val)
            if d:
                return d
    return None


def _classify_ipu_election(rec: Dict) -> str:
    """
    Derive a human-readable election type label from an IPU record.
    Detects snap, runoff, extraordinary, by-election etc.
    """
    # Direct type fields
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


def fetch_ipu_elections(iso2: str) -> Dict[str, Any]:
    """
    Returns a dict:
      lastDate: str | None
      nextDate: str | None
      elections: list of raw election records (for debugging)
      source: str
      notes: str
    """
    if iso2.upper() in IPU_NOT_APPLICABLE:
        reason = IPU_NOT_APPLICABLE[iso2.upper()]
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
        # Parse to compare with today
        try:
            # Handle partial dates (YYYY-MM, YYYY)
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

    # Find the actual next election record to extract its type
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
        "elections": elections[:5],  # keep a few for debugging
        "source": "ipu_parline",
        "notes": f"IPU Parline: {len(elections)} election record(s) found.",
    }

# ── ELECTIONGUIDE SCRAPER (fallback enrichment) ───────────────────────────────

_eg_cache: Optional[Dict[str, List[Dict]]] = None  # iso2 → list of election dicts

def _load_electionguide_cache() -> Dict[str, List[Dict]]:
    """
    Scrape ElectionGuide's past and upcoming election pages.
    Returns a dict mapping country names → list of {date, body, url, status}.
    We then cross-reference with our COUNTRIES list by name.
    """
    global _eg_cache
    if _eg_cache is not None:
        return _eg_cache

    _eg_cache = {}

    if not BS4_AVAILABLE:
        print("  [EG] beautifulsoup4 not available, skipping ElectionGuide scrape")
        return _eg_cache

    # Map ElectionGuide country names → ISO2
    # ElectionGuide uses its own country naming; these are the common mismatches
    EG_NAME_OVERRIDES: Dict[str, str] = {
        "United Kingdom of Great Britain and Northern Ireland": "GB",
        "United Arab Emirates": "AE",
        "Korea, Republic of": "KR",
        "Korea (North)": "KP",
        "Korea, Democratic People's Republic of": "KP",
        "Viet Nam": "VN",
        "Vietnam": "VN",
        "Iran, Islamic Republic of": "IR",
        "Syrian Arab Republic": "SY",
        "Bolivia, Plurinational State of": "BO",
        "Venezuela, Bolivarian Republic of": "VE",
        "Congo (Brazzaville)": "CG",
        "Congo, Democratic Republic of the": "CD",
        "Türkiye": "TR",
        "Turkey": "TR",
        "Russian Federation": "RU",
        "Republic of Korea": "KR",
    }

    # Build reverse map from our COUNTRIES list
    country_name_to_iso2: Dict[str, str] = {c["country"].lower(): c["iso2"] for c in COUNTRIES}
    # Add official name overrides
    for eg_name, iso2 in EG_NAME_OVERRIDES.items():
        country_name_to_iso2[eg_name.lower()] = iso2

    def _name_to_iso2(name: str) -> Optional[str]:
        clean = name.strip().lower()
        if clean in country_name_to_iso2:
            return country_name_to_iso2[clean]
        # Partial match fallback
        for known, code in country_name_to_iso2.items():
            if known in clean or clean in known:
                return code
        return None

    def _parse_eg_page(url: str, status: str) -> None:
        """Parse an ElectionGuide listing page and populate _eg_cache."""
        print(f"  [EG] Scraping {url}")
        html = req_html(url, label=f"ElectionGuide {status}")
        if not html:
            print(f"  [EG] Failed to fetch {url}")
            return

        soup = BeautifulSoup(html, "lxml")

        # ElectionGuide election listings are in <table> rows or structured divs
        # Each row typically has: flag, date+body name, country name
        # The structure as of 2025: rows with class pattern or just td structure
        parsed_count = 0

        # Try table rows first
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # Extract date — usually first or second cell with date text
            date_text = ""
            body_text = ""
            country_text = ""

            for cell in cells:
                text = cell.get_text(separator=" ", strip=True)
                # Date pattern: "Mar 24 2026" or "Mar 24 2026 (d)"
                date_m = re.search(
                    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4}",
                    text,
                )
                if date_m and not date_text:
                    date_text = date_m.group(0)
                # Links give us body name and country
                for a in cell.find_all("a"):
                    href = a.get("href", "")
                    link_text = a.get_text(strip=True)
                    if "/elections/id/" in href and link_text and not body_text:
                        body_text = link_text
                    elif "/countries/id/" in href and link_text and not country_text:
                        country_text = link_text

            if not (date_text and country_text):
                continue

            # Parse date to ISO format
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
    """
    Returns best lastDate / nextDate from ElectionGuide for the given iso2.
    """
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

    # Also capture the body name for the next upcoming election (for type labeling)
    next_record = None
    if future:
        earliest = min(future)
        next_record = next((r for r in records if r.get("date") == earliest), None)

    # Detect snap/runoff/extraordinary from body text
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
    if (not isinstance(payload, list) or len(payload) < 2
            or not isinstance(payload[1], list)):
        return None, None, "Unexpected WB response shape."
    for row in payload[1]:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        dt  = row.get("date")
        if val is None or dt is None:
            continue
        try:
            return float(val), int(dt), None
        except Exception:
            continue
    return None, None, "No non-null value in WB series."

def fetch_wgi(iso2: str) -> Dict[str, Any]:
    components: Dict[str, Any] = {}
    years:  List[int]   = []
    values: List[float] = []
    sources: Dict[str, str] = {}

    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        url = f"{WORLD_BANK_BASE}/country/{iso2.lower()}/indicator/{code}"
        payload = req_json(url, params={"format": "json", "per_page": 60},
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

# ── BUILD ELECTION OBJECT ─────────────────────────────────────────────────────


# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def _clean_wiki(s: Optional[str]) -> Optional[str]:
    """Strip Wikipedia title prefixes and footnote brackets from a name."""
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


def _election_window_active(prev: Optional[Dict], days: int = 7) -> bool:
    """
    Return True if any election date in the previous snapshot falls within
    `days` days before OR after today. Used to trigger daily Claude refresh
    around election periods so results get picked up quickly.
    """
    if not prev:
        return False
    today = datetime.now(timezone.utc).date()
    elec = prev.get("elections") or {}
    candidates = []
    for block in (elec.get("legislative") or {}, elec.get("executive") or {}):
        for key in ("lastElection", "nextElection"):
            obj = block.get(key) if isinstance(block, dict) else None
            if obj:
                candidates.append(obj.get("date"))
        # Also check runoffDate
        for key in ("lastElection", "nextElection"):
            obj = block.get(key) if isinstance(block, dict) else None
            if obj:
                candidates.append(obj.get("runoffDate"))

    for d in candidates:
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
            if abs((dt - today).days) <= days:
                return True
        except (ValueError, AttributeError):
            continue
    return False


def _days_since_claude(prev: Optional[Dict]) -> int:
    """Return days since Claude last updated this country. 999 if never."""
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


def _should_call_claude(iso2: str, wiki_names: Dict, ipu: Dict, eg: Dict,
                        prev: Optional[Dict]) -> Tuple[bool, str]:
    """
    Decide whether to call Claude for this country.
    Returns (should_call, reason).

    Trigger rules (any one fires a call):
      1. No previous snapshot for this country → first run
      2. Wikipedia returned a different leader name than prev snapshot
      3. IPU or EG returned a date not in prev snapshot
      4. Any election date within 7 days of today (before or after) → daily refresh
      5. Last Claude update was >7 days ago → weekly refresh ceiling
      6. CLAUDE_FORCE_REFRESH env var set
    """
    if CLAUDE_FORCE_REFRESH:
        return True, "forced_refresh"

    if not prev:
        return True, "first_run"

    # Trigger 2: Wikipedia name changed
    prev_hos = ((prev.get("executive") or {}).get("headOfState") or {}).get("name")
    prev_hog = ((prev.get("executive") or {}).get("headOfGovernment") or {}).get("name")
    wiki_hos = _clean_wiki(wiki_names.get("hosName"))
    wiki_hog = _clean_wiki(wiki_names.get("hogName"))
    if wiki_hos and wiki_hos != prev_hos:
        return True, f"executive_name_changed ({prev_hos!r} → {wiki_hos!r})"
    if wiki_hog and wiki_hog != prev_hog:
        return True, f"executive_name_changed ({prev_hog!r} → {wiki_hog!r})"

    # Trigger 3: IPU/EG has a date not in prev
    prev_leg_next = ((prev.get("elections") or {}).get("legislative") or {}).get("nextElection") or {}
    ipu_next = ipu.get("nextDate")
    eg_next  = eg.get("nextDate")
    if ipu_next and ipu_next != prev_leg_next.get("date"):
        return True, f"ipu_date_changed ({ipu_next})"
    if eg_next and eg_next != prev_leg_next.get("date"):
        return True, f"eg_date_changed ({eg_next})"

    # Trigger 4: election window active (within 7 days)
    if _election_window_active(prev, days=7):
        return True, "election_window_active"

    # Trigger 5: >7 days since last Claude update
    days_old = _days_since_claude(prev)
    if days_old > 7:
        return True, f"stale_{days_old}d"

    return False, ""


# ── CLAUDE API ─────────────────────────────────────────────────────────────────

import os

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL      = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1400
CLAUDE_FORCE_REFRESH = os.environ.get("CLAUDE_FORCE_REFRESH", "").strip() == "1"

CLAUDE_SYSTEM = """\
You are a political data analyst maintaining a structured JSON dataset of country \
leadership, legislature control, and election data. You will receive:
  - The country name and ISO2 code
  - Whatever the free scrapers found (Wikipedia leader names, IPU/EG election dates)
  - The previous snapshot for this country (may be months old)
  - Today's date

Return ONLY a single valid JSON object — no markdown, no explanation — containing \
the complete, current political block for this country with these exact top-level keys:

{
  "headOfState":        {"name": str, "partyOrGroup": str},
  "headOfGovernment":   {"name": str, "partyOrGroup": str},
  "politicalSystem":    [str, ...],
  "legislature":        [{"name": str, "inControl": str}, ...],
  "competitiveElections": bool,
  "nonCompetitiveReason": str | null,
  "electionsSuspended": bool,
  "suspensionReason":   str | null,
  "legislative": {
    "lastElection": {"date": str, "type": str, "notes": str, "runoffDate": str|null, "runoffCondition": str|null} | null,
    "nextElection": {"date": str, "type": str, "notes": str, "runoffDate": str|null, "runoffCondition": str|null} | null
  },
  "executive": {
    "lastElection": {"date": str, "type": str, "notes": str, "runoffDate": str|null, "runoffCondition": str|null} | null,
    "nextElection": {"date": str, "type": str, "notes": str, "runoffDate": str|null, "runoffCondition": str|null} | null
  },
  "dataAvailabilityNotes": str | null
}

Rules:
- Use the scraper data where available; use your knowledge to fill gaps
- If an election date just passed (date < today), move it to lastElection and \
  update notes with results if known
- notes should be 1-2 sentences, factual, no editorializing
- Preserve previous notes if still accurate rather than rewriting unnecessarily
- Dates: YYYY-MM-DD preferred, YYYY-MM or YYYY if exact date unknown
- For headOfState == headOfGovernment (presidential systems), repeat the same name
- partyOrGroup: official English name of party, or "Non-partisan (monarchy)" etc.
- runoffDate: only if a two-round system is used AND a runoff is scheduled
- If the previous snapshot notes are still accurate, copy them verbatim
- Return ONLY the JSON object, nothing else\
"""


def _call_claude(country_name: str, iso2: str,
                 wiki_names: Dict, ipu: Dict, eg: Dict,
                 prev: Optional[Dict], trigger_reason: str) -> Optional[Dict]:
    """Call the Claude API and return the parsed JSON response, or None on failure."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    today = datetime.now(timezone.utc).date().isoformat()

    # Build a compact context payload
    context = {
        "country": country_name,
        "iso2": iso2,
        "today": today,
        "triggerReason": trigger_reason,
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
        "previousSnapshot": {
            "executive":      prev.get("executive")      if prev else None,
            "politicalSystem":prev.get("politicalSystem") if prev else None,
            "legislature":    prev.get("legislature")     if prev else None,
            "elections":      prev.get("elections")       if prev else None,
            "lastClaudeUpdate": prev.get("lastClaudeUpdate") if prev else None,
        } if prev else None,
    }

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      CLAUDE_MODEL,
                "max_tokens": CLAUDE_MAX_TOKENS,
                "system":     CLAUDE_SYSTEM,
                "messages":   [{"role": "user", "content": json.dumps(context, ensure_ascii=False)}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        raw = ""
        for block in resp.json().get("content", []):
            if block.get("type") == "text":
                raw += block.get("text", "")
        raw = raw.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError(f"Expected dict, got {type(result)}")
        return result
    except json.JSONDecodeError as e:
        print(f"  [{iso2}] ⚠️  Claude JSON parse error: {e}")
    except requests.HTTPError as e:
        print(f"  [{iso2}] ⚠️  Claude HTTP error: {e}")
    except Exception as e:
        print(f"  [{iso2}] ⚠️  Claude error: {e}")
    return None


def _assemble_from_claude(iso2: str, cl: Dict, ipu: Dict, eg: Dict,
                          trigger: str, today_str: str) -> Tuple[Dict, Dict, Dict]:
    """
    Turn Claude's response dict into (executive_block, legislature_block, elections_block).
    """

    def _norm_election(obj: Optional[Dict]) -> Optional[Dict]:
        if not obj:
            return None
        return {
            "date":           obj.get("date"),
            "type":           obj.get("type"),
            "notes":          obj.get("notes"),
            "runoffDate":     obj.get("runoffDate"),
            "runoffCondition":obj.get("runoffCondition"),
            "electionDay":    str(obj.get("date", "")) == today_str,
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

    election_today = (str((cl_leg.get("nextElection") or {}).get("date", "")) == today_str or
                      str((cl_exec.get("nextElection") or {}).get("date", "")) == today_str)

    # Prefer IPU/EG date over Claude if more precise and same year
    def _maybe_refine_date(election_obj: Optional[Dict], scraper_date: Optional[str]) -> Optional[Dict]:
        if not election_obj or not scraper_date or len(scraper_date) != 10:
            return election_obj
        static_year = str((election_obj.get("date") or ""))[:4]
        if scraper_date.startswith(static_year):
            election_obj = dict(election_obj)
            election_obj["date"] = scraper_date
        return election_obj

    leg_next = _maybe_refine_date(leg_next, ipu.get("nextDate") or eg.get("nextDate"))

    elections_block = {
        "competitiveElections": cl.get("competitiveElections", True),
        "nonCompetitiveReason": cl.get("nonCompetitiveReason"),
        "electionsSuspended":   cl.get("electionsSuspended", False),
        "suspensionReason":     cl.get("suspensionReason"),
        "electionToday":        election_today,
        "legislative": {
            "lastElection": _norm_election(cl_leg.get("lastElection")),
            "nextElection": leg_next,
            "source":       f"claude ({trigger})" + (
                " + ipu_parline" if ipu.get("nextDate") else "") + (
                " + electionguide" if eg.get("nextDate") else ""),
        },
        "executive": {
            "lastElection": _norm_election(cl_exec.get("lastElection")),
            "nextElection": exec_next,
            "source":       f"claude ({trigger})",
        },
    }

    return executive_block, legislature_block, elections_block


def build_country(name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
    prev = prev_by_iso2.get(iso2)
    today_str = datetime.now(timezone.utc).date().isoformat()

    # ── Free scrapers ─────────────────────────────────────────────────────────
    print(f"  [{iso2}] Wikipedia lookup...")
    wiki = _load_wiki_exec_cache().get(iso2, {})
    print(f"  [{iso2}] HOS={_clean_wiki(wiki.get('hosName'))}, HOG={_clean_wiki(wiki.get('hogName'))}")

    print(f"  [{iso2}] IPU elections fetch...")
    ipu = fetch_ipu_elections(iso2)
    print(f"  [{iso2}] IPU: last={ipu.get('lastDate')} next={ipu.get('nextDate')} src={ipu.get('source')}")

    print(f"  [{iso2}] ElectionGuide lookup...")
    eg = get_electionguide_dates(iso2)
    print(f"  [{iso2}] EG: last={eg.get('lastDate')} next={eg.get('nextDate')}")

    print(f"  [{iso2}] World Bank WGI fetch...")
    wb_gov = merge_wb_sticky(fetch_wgi(iso2), prev)

    print(f"  [{iso2}] REST Countries fetch...")
    meta = fetch_rest_countries(iso2)

    # ── Claude trigger decision ───────────────────────────────────────────────
    should_call, trigger_reason = _should_call_claude(iso2, wiki, ipu, eg, prev)

    if should_call:
        print(f"  [{iso2}] 🤖 Claude triggered: {trigger_reason}")
        cl = _call_claude(name, iso2, wiki, ipu, eg, prev, trigger_reason)
    else:
        cl = None
        print(f"  [{iso2}] ✓  Claude skipped (last updated {_days_since_claude(prev)}d ago)")

    # ── Assemble output ───────────────────────────────────────────────────────
    if cl:
        executive_block, legislature_block, elections_block = \
            _assemble_from_claude(iso2, cl, ipu, eg, trigger_reason, today_str)
        pol_sys = {"values": cl.get("politicalSystem", ["unknown"]),
                   "source": f"claude ({trigger_reason})"}
        data_avail_note = cl.get("dataAvailabilityNotes")
        last_claude_update = iso_z(now_utc())
    elif prev:
        # No Claude call — carry forward previous political data unchanged
        print(f"  [{iso2}] ↩  Carrying forward previous political data")
        executive_block  = prev.get("executive",      {})
        legislature_block= prev.get("legislature",    {})
        elections_block  = prev.get("elections",      {})
        pol_sys          = prev.get("politicalSystem", {"values": ["unknown"], "source": "carried_forward"})
        data_avail_note  = (prev.get("dataAvailability") or {}).get("executive")
        last_claude_update = prev.get("lastClaudeUpdate")
    else:
        # No Claude, no previous data — bare skeleton
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
            "competitiveElections": None, "nonCompetitiveReason": None,
            "electionsSuspended": False, "suspensionReason": None,
            "electionToday": False,
            "legislative": {"lastElection": None, "nextElection": None, "source": "unknown"},
            "executive":   {"lastElection": None, "nextElection": None, "source": "unknown"},
        }
        pol_sys = {"values": ["unknown"], "source": "unknown"}
        data_avail_note = None
        last_claude_update = None

    # World Bank availability note
    avail: Dict[str, str] = {}
    if data_avail_note:
        avail["executive"] = data_avail_note
    if wb_gov.get("overallPercentile") is None:
        avail["worldBankGovernance"] = f"World Bank WGI data unavailable for '{iso2}'."
    if not meta.get("capital") and not meta.get("population"):
        avail["metadata"] = f"REST Countries API returned no data for '{iso2}'."

    return {
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
        "politicalSystem":    pol_sys,
        "executive":          executive_block,
        "legislature":        legislature_block,
        "worldBankGovernance":wb_gov,
        "dataAvailability":   avail if avail else None,
        "elections":          elections_block,
        "lastClaudeUpdate":   last_claude_update,
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("docs") / "countries_snapshot.json"
    prev = load_previous_snapshot(out_path)
    print(f"=== Starting build. Previous snapshot: {len(prev)} countries cached ===")

    # Pre-load shared caches (one HTTP round-trip each, reused for all countries)
    _load_ipu_parliament_map()
    _load_electionguide_cache()
    _load_wiki_exec_cache()

    out = {
        "generatedAt":        iso_z(now_utc()),
        "worldBankYearRule":  "latest_non_null_per_indicator",
        "countries":          [],
        "sources": {
            "executives":            "wikipedia_adaptive + claude_api (smart diff, 7d ceiling)",
            "legislature":           "claude_api (smart diff, 7d ceiling)",
            "elections":             "ipu_parline + electionguide + claude_api (daily near elections)",
            "wikipedia_adaptive":    WIKIPEDIA_API,
            "world_bank_base":       WORLD_BANK_BASE,
            "ipu_parline":           f"{IPU_API_BASE}/api",
            "electionguide":         ELECTIONGUIDE_BASE,
            "rest_countries":        REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
        "electionDataModel": {
            "description": (
                "Each country's elections block contains competitiveElections (bool), "
                "nonCompetitiveReason (str|null for one-party/non-democratic states), "
                "and legislative/executive sub-blocks each with lastElection and nextElection. "
                "Election objects contain: date (ISO string or year), type (string description), "
                "notes (context string)."
            ),
        },
    }

    for c in COUNTRIES:
        print(f"\n▶ {c['country']} ({c['iso2']})")
        country_data = build_country(c["country"], c["iso2"], prev)
        out["countries"].append(country_data)
        time.sleep(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")


if __name__ == "__main__":
    main()
