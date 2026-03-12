"""
Build a Base44-friendly JSON snapshot for multiple countries.
Output: public/countries_snapshot.json

Run:  python scripts/build_countries_snapshot.py
Deps: pip install requests
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

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

WIKIDATA_SPARQL     = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE     = "https://api.worldbank.org/v2"
IPU_API_BASE        = "https://api.data.ipu.org/v1"
REST_COUNTRIES_BASE = "https://restcountries.com/v3.1"
WIKIPEDIA_API       = "https://en.wikipedia.org/w/api.php"

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

# Taiwan is not in IPU (not a UN member state). None = skip gracefully.
IPU_ISO2_OVERRIDES: Dict[str, Optional[str]] = {
    "TW": None,
}

# ── WIKIPEDIA COUNTRY NAME MAP ────────────────────────────────────────────────
# Maps ISO2 → exact country name as it appears in the Wikipedia HOS/HOG list.
# Only needed for countries whose name differs from our display name.
WIKIPEDIA_COUNTRY_NAME_MAP: Dict[str, str] = {
    "RU": "Russia",
    "IN": "India",
    "PK": "Pakistan",
    "CN": "China",
    "GB": "United Kingdom",
    "DE": "Germany",
    "AE": "United Arab Emirates",
    "SA": "Saudi Arabia",
    "IL": "Israel",
    "PS": "Palestine",
    "MX": "Mexico",
    "BR": "Brazil",
    "CA": "Canada",
    "NG": "Nigeria",
    "JP": "Japan",
    "IR": "Iran",
    "SY": "Syria",
    "FR": "France",
    "TR": "Turkey",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "TW": "Taiwan",  # Not in list but kept for fallback
    "KR": "South Korea",
    "KP": "North Korea",
    "ID": "Indonesia",
    "MM": "Myanmar",
    "AM": "Armenia",
    "AZ": "Azerbaijan",
    "MA": "Morocco",
    "SO": "Somalia",
    "YE": "Yemen",
    "LY": "Libya",
    "EG": "Egypt",
    "DZ": "Algeria",
    "AR": "Argentina",
    "CL": "Chile",
    "PE": "Peru",
    "CU": "Cuba",
    "CO": "Colombia",
    "PA": "Panama",
    "SV": "El Salvador",
    "DK": "Denmark",
    "SD": "Sudan",
    "UA": "Ukraine",
}

# ── STATIC EXECUTIVE OVERRIDES ────────────────────────────────────────────────
# Applied AFTER Wikipedia fetch. Use for:
#   - Party/group affiliations (Wikipedia list rarely includes these)
#   - Countries not in the Wikipedia list (Taiwan, disputed states)
#   - Force-correct known Wikipedia parsing failures
# Keys are ISO2 codes. Only listed fields are overridden; others come from Wikipedia.

STATIC_EXECUTIVE_OVERRIDES: Dict[str, Dict[str, Optional[str]]] = {
    # ── Party overrides (Wikipedia names but not parties) ──
    "RU": {"hosParty": "United Russia", "hogParty": "United Russia"},
    "CN": {"hosParty": "Chinese Communist Party", "hogParty": "Chinese Communist Party"},
    "VN": {"hosParty": "Communist Party of Vietnam", "hogParty": "Communist Party of Vietnam"},
    "KP": {"hosParty": "Korean Workers' Party", "hogParty": "Korean Workers' Party"},
    "CU": {"hosParty": "Communist Party of Cuba", "hogParty": "Communist Party of Cuba"},
    "NG": {"hosParty": "All Progressives Congress", "hogParty": "All Progressives Congress"},
    "FR": {"hosParty": "Renaissance", "hogParty": "Renaissance"},
    "TR": {"hosParty": "Justice and Development Party", "hogParty": "Justice and Development Party"},
    "IN": {"hosParty": "Bharatiya Janata Party", "hogParty": "Bharatiya Janata Party"},
    "MX": {"hosParty": "Morena", "hogParty": "Morena"},
    "AR": {"hosParty": "La Libertad Avanza", "hogParty": "La Libertad Avanza"},
    "BR": {"hosParty": "Workers' Party", "hogParty": "Workers' Party"},
    "SA": {"hosParty": "House of Saud (monarchy)", "hogParty": "House of Saud (monarchy)"},
    "AE": {"hosParty": "Al Nahyan family (monarchy)", "hogParty": "Al Nahyan family (monarchy)"},
    "EG": {"hosParty": "No party (military)", "hogParty": "No party (military)"},
    "DZ": {"hosParty": "National Liberation Front", "hogParty": "National Liberation Front"},
    "PS": {"hogParty": "Fatah"},
    "UA": {"hosParty": "Servant of the People", "hogParty": "Servant of the People"},
    "DE": {"hosParty": "Social Democratic Party", "hogParty": "Christian Democratic Union"},
    "GB": {"hosParty": "Monarchy (non-partisan)", "hogParty": "Labour Party"},
    "CA": {"hosParty": "Monarchy (non-partisan)", "hogParty": "Liberal Party"},
    "DK": {"hosParty": "Monarchy (non-partisan)", "hogParty": "Social Democrats"},
    "JP": {"hosParty": "Imperial House (non-partisan)", "hogParty": "Liberal Democratic Party"},
    # ── Name + party overrides for complex/disputed situations ──
    # Venezuela: Maduro fled Jan 2025; Delcy Rodríguez acting president Jan 3 2026
    "VE": {
        "hosName":  "Delcy Rodríguez (acting)",
        "hosParty": "United Socialist Party of Venezuela",
        "hogName":  "Delcy Rodríguez (acting)",
        "hogParty": "United Socialist Party of Venezuela",
    },
    # Syria: transitional govt post-Assad (Dec 2024)
    "SY": {
        "hosName":  "Ahmad al-Sharaa",
        "hosParty": "Hayat Tahrir al-Sham (transitional)",
        "hogName":  "Mohammad al-Bashir",
        "hogParty": "Hayat Tahrir al-Sham (transitional)",
    },
    # Iran: Ali Khamenei killed ~Feb 28 2026; interim leadership council in place
    "IR": {
        "hosName":  "Interim Leadership Council",
        "hosParty": "Islamic Republic (transitional)",
        "hogName":  "Masoud Pezeshkian",
        "hogParty": "Reformist",
    },
    # South Korea: Lee Jae-myung won June 2025 election after Yoon impeachment
    "KR": {
        "hosName":  "Lee Jae-myung",
        "hosParty": "Democratic Party of Korea",
        "hogName":  "Lee Jae-myung",
        "hogParty": "Democratic Party of Korea",
    },
    # Taiwan: not in Wikipedia list (disputed sovereignty)
    "TW": {
        "hosName":  "Lai Ching-te",
        "hosParty": "Democratic Progressive Party",
        "hogName":  "Cho Jung-tai",
        "hogParty": "Democratic Progressive Party",
    },
    # Myanmar: military junta post-2021 coup
    "MM": {
        "hosName":  "Min Aung Hlaing",
        "hosParty": "Tatmadaw (military junta)",
        "hogName":  "Min Aung Hlaing",
        "hogParty": "Tatmadaw (military junta)",
    },
}

# ── DATA AVAILABILITY NOTES ───────────────────────────────────────────────────

DATA_AVAILABILITY_NOTES: Dict[str, Dict[str, str]] = {
    "TW": {
        "worldBankGovernance": (
            "Taiwan is not a UN member state and is not recognised by the World Bank. "
            "WGI data is unavailable."
        ),
        "elections.legislative": (
            "Taiwan is not a member of the IPU and is absent from the Parline database."
        ),
    },
    "PS": {
        "worldBankGovernance": (
            "World Bank data for Palestine is limited due to its political status."
        ),
    },
    "KP": {
        "worldBankGovernance": (
            "North Korea governance data is based on limited external assessments."
        ),
        "elections.legislative": (
            "North Korea holds nominal single-party elections; IPU may not track these."
        ),
    },
    "SY": {
        "executive": (
            "Syria's transitional government (post-Assad, Dec 2024) is not yet in Wikidata. "
            "Executive data is from static overrides."
        ),
    },
    "SO": {
        "worldBankGovernance": (
            "Somalia governance data is based on limited external assessments."
        ),
    },
    "YE": {
        "worldBankGovernance": (
            "Yemen governance data reflects the pre-conflict baseline; "
            "current effective governance is severely disrupted by civil war."
        ),
    },
    "LY": {
        "worldBankGovernance": (
            "Libya has parallel governing authorities; data reflects the "
            "internationally recognised government's institutional capacity."
        ),
    },
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    label: str = "",
) -> Optional[Any]:
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

# ── WIKIDATA ──────────────────────────────────────────────────────────────────

def wikidata_sparql(query: str) -> Optional[dict]:
    return req_json(
        WIKIDATA_SPARQL,
        params={"format": "json", "query": query},
        headers={"Accept": "application/sparql-results+json"},
        label="Wikidata SPARQL",
    )

def _wd(b: dict, key: str) -> Optional[str]:
    v = b.get(key)
    return v.get("value") if v else None

def get_qid(iso2: str) -> Optional[str]:
    data = wikidata_sparql(f'SELECT ?c WHERE {{ ?c wdt:P297 "{iso2}" . }} LIMIT 1')
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return None
    uri = _wd(bindings[0], "c")
    return uri.rsplit("/", 1)[-1] if uri else None

def get_political_systems(qid: str) -> List[str]:
    data = wikidata_sparql(f"""
    SELECT ?psLabel WHERE {{
      wd:{qid} wdt:P122 ?ps .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }} LIMIT 20
    """)
    out: List[str] = []
    for b in safe_get(data, "results", "bindings", default=[]):
        lbl = _wd(b, "psLabel")
        if lbl and lbl not in out:
            out.append(lbl)
    return out

# ── WIKIPEDIA EXECUTIVE LOOKUP ────────────────────────────────────────────────

_wiki_exec_cache: Optional[Dict[str, Dict[str, Optional[str]]]] = None

def _load_wiki_exec_cache() -> Dict[str, Dict[str, Optional[str]]]:
    """
    Fetch Wikipedia's 'List of current heads of state and government' via the
    MediaWiki parse API, parse the HTML table, and return a dict keyed by
    country name → {hosName, hogName}.

    The Wikipedia table has columns: Country | Head of State | Head of Government
    Some rows have the same person for both (presidential systems).
    """
    global _wiki_exec_cache
    if _wiki_exec_cache is not None:
        return _wiki_exec_cache

    print("  [WIKI] Fetching Wikipedia heads of state/government list...")

    try:
        from html.parser import HTMLParser
    except ImportError:
        print("  [WIKI] html.parser not available")
        _wiki_exec_cache = {}
        return _wiki_exec_cache

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
        print("  [WIKI] Failed to fetch Wikipedia list")
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    html_text = safe_get(data, "parse", "text", default="")
    if not html_text:
        print("  [WIKI] Empty HTML from Wikipedia API")
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    # Parse with stdlib html.parser — no BeautifulSoup dependency needed
    result: Dict[str, Dict[str, Optional[str]]] = {}

    class TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.in_cell = False
            self.current_row: List[str] = []
            self.current_cell_parts: List[str] = []
            self.depth = 0
            self.skip_depth = 0  # for nested elements we want to skip
            self.rows: List[List[str]] = []

        def _cell_text(self) -> str:
            raw = " ".join(self.current_cell_parts).strip()
            # Collapse whitespace, strip footnote markers like [1], [2]
            raw = re.sub(r"\[\d+\]", "", raw)
            raw = re.sub(r"\s+", " ", raw).strip()
            return raw

        def handle_starttag(self, tag, attrs):
            attrs_dict = dict(attrs)
            if tag == "table":
                cls = attrs_dict.get("class", "")
                if "wikitable" in cls:
                    self.in_table = True
                    self.depth = 0
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

    for row in parser.rows:
        if len(row) < 2:
            continue
        # Row format: [Country, Head of State, Head of Government] or [Country, Same person]
        country_raw = row[0].strip()
        if not country_raw or country_raw.lower() in ("country", "state", ""):
            continue

        hos_raw = row[1].strip() if len(row) > 1 else ""
        hog_raw = row[2].strip() if len(row) > 2 else hos_raw

        # Clean up: take first name if cell has multiple (e.g. "Name1 | Name2")
        def _first_name(s: str) -> Optional[str]:
            if not s:
                return None
            # Split on pipe separator we inserted at <br>
            parts = [p.strip() for p in s.split("|") if p.strip()]
            name = parts[0] if parts else s
            # Remove trailing role descriptions in parentheses if very long
            name = re.sub(r"\s*\((?:acting|interim|transitional|designate)[^)]*\)", 
                          lambda m: m.group(0), name, flags=re.IGNORECASE)
            return name.strip() or None

        result[country_raw] = {
            "hosName": _first_name(hos_raw),
            "hogName": _first_name(hog_raw),
        }

    print(f"  [WIKI] Parsed {len(result)} countries from Wikipedia executive list")
    if result:
        # Print a few samples for diagnostics
        samples = list(result.items())[:5]
        for k, v in samples:
            print(f"  [WIKI]   {k}: HOS={v['hosName']}, HOG={v['hogName']}")

    _wiki_exec_cache = result
    return result


def get_wiki_executive(iso2: str) -> Dict[str, Optional[str]]:
    """
    Look up head of state and head of government from the Wikipedia list.
    Returns dict with hosName and hogName (or None if not found).
    Falls back gracefully — the static overrides layer handles the rest.
    """
    cache = _load_wiki_exec_cache()
    wiki_name = WIKIPEDIA_COUNTRY_NAME_MAP.get(iso2, "")
    if not wiki_name:
        return {"hosName": None, "hogName": None}

    # Direct match
    entry = cache.get(wiki_name)
    if entry:
        return entry

    # Case-insensitive fuzzy match as fallback
    wiki_lower = wiki_name.lower()
    for key, val in cache.items():
        if key.lower() == wiki_lower or wiki_lower in key.lower():
            print(f"  [WIKI] Fuzzy match '{wiki_name}' → '{key}'")
            return val

    print(f"  [WIKI] No match for '{wiki_name}' (iso2={iso2})")
    return {"hosName": None, "hogName": None}

def get_legislature_bodies(qid: str) -> List[str]:
    data = wikidata_sparql(f"""
    SELECT ?legLabel WHERE {{
      wd:{qid} wdt:P194 ?leg .
      ?leg wdt:P31/wdt:P279* wd:Q11204 .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """)
    out: List[str] = []
    for b in safe_get(data, "results", "bindings", default=[]):
        lbl = _wd(b, "legLabel")
        if lbl and lbl not in out:
            out.append(lbl)
    return out

def _today_dt() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def get_next_election_wikidata(qid: str, kind: str) -> Dict[str, Any]:
    """
    KEY FIX: UNION of wdt:P1001 (applies to jurisdiction) and wdt:P17 (country).
    Most election items in Wikidata use P17, not P1001.
    Also includes broad Q40231 (election) as fallback type.
    """
    today = _today_dt()
    type_values = ("wd:Q159821 wd:Q152203 wd:Q40231"
                   if kind == "executive"
                   else "wd:Q1079032 wd:Q104203 wd:Q152203 wd:Q40231")

    data = wikidata_sparql(f"""
    SELECT ?eLabel ?date ?typeLabel WHERE {{
      {{ ?e wdt:P1001 wd:{qid} . }} UNION {{ ?e wdt:P17 wd:{qid} . }}
      ?e wdt:P585 ?date .
      FILTER(?date >= "{today}"^^xsd:dateTime)
      ?e wdt:P31 ?type .
      VALUES ?type {{ {type_values} }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"exists": "unknown", "nextDate": None, "electionType": None,
                "method": "wikidata_upcoming",
                "notes": "No upcoming election found in Wikidata (P1001/P17 UNION)."}
    b = bindings[0]
    return {"exists": True, "nextDate": _wd(b, "date"),
            "electionType": _wd(b, "typeLabel"),
            "method": "wikidata_upcoming",
            "notes": "From Wikidata upcoming election items (P1001/P17 UNION + future date)."}

def get_last_leg_winner(qid: str) -> Dict[str, Any]:
    today = _today_dt()
    data = wikidata_sparql(f"""
    SELECT ?eLabel ?date ?winnerLabel WHERE {{
      {{ ?e wdt:P1001 wd:{qid} . }} UNION {{ ?e wdt:P17 wd:{qid} . }}
      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)
      ?e wdt:P31 ?type .
      VALUES ?type {{ wd:Q152203 wd:Q1079032 wd:Q104203 wd:Q40231 }}
      OPTIONAL {{ ?e wdt:P1346 ?winner . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"winner": "unknown", "method": "wikidata_last_leg_election_winner",
                "notes": "No prior legislative election found in Wikidata (P1001/P17 UNION)."}
    b = bindings[0]
    return {
        "winner": _wd(b, "winnerLabel") or "unknown",
        "method": "wikidata_last_leg_election_winner",
        "notes": "Last national legislative election winner via Wikidata P1346 (best-effort).",
        "basis": {"electionName": _wd(b, "eLabel"), "electionDate": _wd(b, "date")},
    }

# ── IPU PARLINE ───────────────────────────────────────────────────────────────

_ipu_cache: Optional[Dict[str, List[Dict]]] = None

def _load_ipu_cache() -> Dict[str, List[Dict]]:
    global _ipu_cache
    if _ipu_cache is not None:
        return _ipu_cache

    print("  [IPU] Pre-fetching chamber data for all countries...")
    cache: Dict[str, List[Dict]] = {}
    _first_logged = False

    for c in COUNTRIES:
        iso2 = c["iso2"]
        if IPU_ISO2_OVERRIDES.get(iso2) is None and iso2 in IPU_ISO2_OVERRIDES:
            print(f"  [IPU] Skipping {iso2} (not in IPU)")
            continue

        url = f"{IPU_API_BASE}/chambers/{iso2.upper()}"
        data = req_json(url, label=f"IPU /chambers/{iso2}")
        if not data:
            print(f"  [IPU] {iso2}: no data returned")
            continue

        # ── DIAGNOSTIC: log shape of first successful response ──
        if not _first_logged:
            if isinstance(data, dict):
                top = list(data.keys())[:10]
                inner = data.get("data")
                print(f"  [IPU] FIRST RESPONSE SHAPE ({iso2}): dict, top_keys={top}, "
                      f"data_type={type(inner).__name__}")
                if isinstance(inner, list) and inner:
                    print(f"  [IPU]   data[0] keys: {list(inner[0].keys())[:12]}")
                elif isinstance(inner, dict):
                    print(f"  [IPU]   data dict keys: {list(inner.keys())[:12]}")
            elif isinstance(data, list):
                print(f"  [IPU] FIRST RESPONSE SHAPE ({iso2}): list[{len(data)}]")
                if data and isinstance(data[0], dict):
                    print(f"  [IPU]   [0] keys: {list(data[0].keys())[:12]}")
            _first_logged = True

        # ── Parse all possible response shapes ──
        chambers: List[Dict] = []
        if isinstance(data, list):
            chambers = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            raw = data.get("data")
            if isinstance(raw, list):
                chambers = [r for r in raw if isinstance(r, dict)]
            elif isinstance(raw, dict):
                chambers = [raw]
            elif raw is None:
                # Flat dict — check for known field names
                if any(k in data for k in ("last_election_date",
                                            "expect_date_next_election",
                                            "country_code")):
                    chambers = [data]
                else:
                    print(f"  [IPU] {iso2}: unrecognised flat dict, "
                          f"keys={list(data.keys())[:12]}")

        # Unwrap JSON:API attributes layer if present
        unwrapped: List[Dict] = []
        for ch in chambers:
            attrs = ch.get("attributes")
            unwrapped.append(attrs if isinstance(attrs, dict) else ch)

        if unwrapped:
            first = unwrapped[0]
            date_fields = {k: v for k, v in first.items()
                           if "date" in k.lower() or "election" in k.lower()}
            print(f"  [IPU] {iso2}: {len(unwrapped)} chamber(s), "
                  f"date/election fields={date_fields}")
            cache[iso2.upper()] = unwrapped
        else:
            print(f"  [IPU] {iso2}: parsed to 0 chambers")

        time.sleep(0.15)

    print(f"  [IPU] Cache complete: {len(cache)} countries with data")
    _ipu_cache = cache
    return cache

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

def fetch_ipu_leg_election(iso2: str) -> Dict[str, Any]:
    if IPU_ISO2_OVERRIDES.get(iso2) is None and iso2 in IPU_ISO2_OVERRIDES:
        return {"exists": "unknown", "lastDate": None, "nextDate": None,
                "chamberName": None, "chamberType": None, "method": "ipu_parline",
                "notes": f"{iso2} not represented in IPU Parline."}

    chambers = _load_ipu_cache().get(iso2.upper(), [])
    if not chambers:
        return {"exists": "unknown", "lastDate": None, "nextDate": None,
                "chamberName": None, "chamberType": None, "method": "ipu_parline",
                "notes": f"No IPU chamber data found for {iso2}."}

    def _priority(ch: dict) -> int:
        s = str(ch.get("struct_parl_status") or "").lower()
        if "lower" in s or "unicameral" in s: return 0
        if "upper" in s: return 1
        return 2

    best = sorted(chambers, key=_priority)[0]
    suspended = bool(best.get("is_suspended_chamber", False))
    next_date = _parse_ipu_date(best.get("expect_date_next_election"))
    last_date = _parse_ipu_date(best.get("last_election_date"))

    return {
        "exists":      True if (next_date or last_date) else "unknown",
        "lastDate":    last_date,
        "nextDate":    next_date,
        "chamberName": best.get("election_title") or best.get("country_name"),
        "chamberType": best.get("struct_parl_status"),
        "method":      "ipu_parline",
        "notes": ("Chamber suspended per IPU." if suspended
                  else "From IPU Parline parliamentary election schedule."),
    }

# ── REST COUNTRIES ────────────────────────────────────────────────────────────

def fetch_rest_countries(iso2: str) -> Dict[str, Any]:
    """
    IMPORTANT: Do NOT add ?fields= parameter.
    REST Countries v3.1 returns a plain dict (not a list) when fields are
    specified, which breaks the list-unwrap below.
    """
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
            "notes": f"No REST Countries data for {iso2}.",
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
        "notes":        None,
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
        kept["notes"] = (f"Kept previous values; latest fetch failed: "
                         f"{new_wb.get('notes')}")
        return kept
    out = dict(new_wb)
    out.pop("ok", None)
    return out

# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def build_country(name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
    print(f"  [{iso2}] QID lookup...")
    qid = get_qid(iso2)
    print(f"  [{iso2}] QID={qid}")

    pol_sys    = ["unknown"]
    leg_bodies: List[str] = []
    leg_winner = {"winner": "unknown", "method": "wikidata_last_leg_election_winner",
                  "notes": "QID not found"}
    exec_elec  = {"exists": "unknown", "nextDate": None, "electionType": None,
                  "method": "wikidata_upcoming", "notes": "QID not found"}

    if qid:
        pol_sys    = get_political_systems(qid) or ["unknown"]
        leg_bodies = get_legislature_bodies(qid)
        leg_winner = get_last_leg_winner(qid)
        exec_elec  = get_next_election_wikidata(qid, "executive")

    # ── Wikipedia executive lookup (primary source for names) ────────────────
    print(f"  [{iso2}] Wikipedia executive lookup...")
    wiki_exec = get_wiki_executive(iso2)
    print(f"  [{iso2}] Wikipedia: HOS={wiki_exec.get('hosName')}, HOG={wiki_exec.get('hogName')}")

    # ── Apply static overrides ────────────────────────────────────────────────
    ov = STATIC_EXECUTIVE_OVERRIDES.get(iso2, {})
    if ov:
        print(f"  [{iso2}] Applying static override: {list(ov.keys())}")

    # Wikipedia provides names; static overrides provide parties (and name fixes)
    hos_name  = ov.get("hosName")  or wiki_exec.get("hosName")
    hos_party = ov.get("hosParty") or "unknown"
    hog_name  = ov.get("hogName")  or wiki_exec.get("hogName")
    hog_party = ov.get("hogParty") or "unknown"
    exec_leader = hog_name or hos_name
    exec_party  = hog_party if hog_party != "unknown" else hos_party
    exec_src    = "wikipedia_hos_hog_list"
    if ov.get("hosName") or ov.get("hogName"):
        exec_src = "static_override"
    elif ov:
        exec_src = "wikipedia_hos_hog_list+party_override"

    # ── IPU legislative elections ─────────────────────────────────────────────
    print(f"  [{iso2}] IPU fetch...")
    ipu = fetch_ipu_leg_election(iso2)
    print(f"  [{iso2}] IPU: lastDate={ipu.get('lastDate')}, nextDate={ipu.get('nextDate')}")

    if ipu.get("nextDate"):
        leg_elec = {
            "exists":       ipu["exists"],
            "lastDate":     ipu["lastDate"],
            "nextDate":     ipu["nextDate"],
            "electionType": ipu["chamberType"],
            "method":       "ipu_parline",
            "notes":        ipu["notes"],
            "source":       "IPU Parline API",
        }
    else:
        wd_leg = get_next_election_wikidata(qid, "legislative") if qid else {}
        leg_elec = {
            "exists":       wd_leg.get("exists", "unknown"),
            "lastDate":     ipu.get("lastDate"),
            "nextDate":     wd_leg.get("nextDate"),
            "electionType": wd_leg.get("electionType"),
            "method":       "ipu_parline+wikidata_fallback",
            "notes":        (f"IPU had no nextDate ({ipu.get('notes', '')}); "
                             f"nextDate from Wikidata fallback."),
            "source":       "IPU Parline (lastDate) + Wikidata (nextDate fallback)",
        }

    # ── Legislature bodies ────────────────────────────────────────────────────
    bodies = leg_bodies or ["Legislature"]
    legislature = [
        {
            "name":          b,
            "inControl":     leg_winner.get("winner", "unknown"),
            "controlMethod": leg_winner.get("method"),
            "controlNotes":  leg_winner.get("notes"),
            "controlBasis":  leg_winner.get("basis"),
        }
        for b in bodies
    ]

    # ── World Bank WGI ────────────────────────────────────────────────────────
    print(f"  [{iso2}] WB WGI fetch...")
    new_wb = fetch_wgi(iso2)
    wb_gov = merge_wb_sticky(new_wb, prev_by_iso2.get(iso2))

    # ── REST Countries metadata ───────────────────────────────────────────────
    print(f"  [{iso2}] REST Countries fetch...")
    meta = fetch_rest_countries(iso2)
    print(f"  [{iso2}] meta: capital={meta.get('capital')}, pop={meta.get('population')}")

    # ── dataAvailability block ────────────────────────────────────────────────
    static_notes = DATA_AVAILABILITY_NOTES.get(iso2, {})
    avail: Dict[str, str] = {}

    if not qid:
        avail["executive"] = (
            static_notes.get("executive") or
            f"No Wikidata QID found for '{iso2}'. Executive data unavailable.")
    elif static_notes.get("executive"):
        avail["executive"] = static_notes["executive"]

    if wb_gov.get("overallPercentile") is None:
        avail["worldBankGovernance"] = (
            static_notes.get("worldBankGovernance") or
            f"World Bank WGI data unavailable for '{iso2}'.")
    elif static_notes.get("worldBankGovernance"):
        avail["worldBankGovernance"] = static_notes["worldBankGovernance"]

    if not leg_elec.get("nextDate") and not leg_elec.get("lastDate"):
        avail["elections.legislative"] = (
            static_notes.get("elections.legislative") or
            f"No legislative election data in IPU or Wikidata for '{iso2}'.")
    elif static_notes.get("elections.legislative"):
        avail["elections.legislative"] = static_notes["elections.legislative"]

    if meta.get("capital") is None and meta.get("population") is None:
        avail["metadata"] = f"REST Countries API returned no data for '{iso2}'."

    # ── Assemble final record ─────────────────────────────────────────────────
    return {
        "country": name,
        "iso2":    iso2,
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
        "politicalSystem": {
            "values": pol_sys,
            "source": "wikidata:P122",
        },
        "executive": {
            "headOfState": {
                "name":         hos_name,
                "partyOrGroup": hos_party,
                "source": ("static_override"
                           if ov.get("hosName") else
                           "wikipedia:List_of_current_heads_of_state_and_government"
                           + (" + party_static_override" if ov.get("hosParty") else "")),
            },
            "headOfGovernment": {
                "name":         hog_name,
                "partyOrGroup": hog_party,
                "source": ("static_override"
                           if ov.get("hogName") else
                           "wikipedia:List_of_current_heads_of_state_and_government"
                           + (" + party_static_override" if ov.get("hogParty") else "")),
            },
            "executiveInPower": {
                "leader":       exec_leader,
                "partyOrGroup": exec_party,
                "method":       exec_src,
            },
        },
        "legislature": {
            "bodies": legislature,
            "source": ("wikidata:P194 (filtered to legislature items) "
                       "+ control best-effort via elections winner P1346"),
        },
        "worldBankGovernance": wb_gov,
        "dataAvailability": avail if avail else None,
        "elections": {
            "legislative": leg_elec,
            "executive": {
                "exists":       exec_elec["exists"],
                "nextDate":     exec_elec["nextDate"],
                "electionType": exec_elec["electionType"],
                "method":       exec_elec["method"],
                "notes":        exec_elec["notes"],
                "source":       "wikidata: P1001/P17 UNION, P585, P31",
            },
        },
    }

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("public") / "countries_snapshot.json"
    prev = load_previous_snapshot(out_path)
    print(f"=== Starting build. Previous snapshot: {len(prev)} countries cached ===")

    _load_ipu_cache()
    _load_wiki_exec_cache()

    out = {
        "generatedAt":        iso_z(now_utc()),
        "worldBankYearRule":  "latest_non_null_per_indicator",
        "countries":          [],
        "sources": {
            "wikipedia_executives": WIKIPEDIA_API,
            "wikidata_sparql": WIKIDATA_SPARQL,
            "world_bank_base": WORLD_BANK_BASE,
            "ipu_parline":     IPU_API_BASE,
            "rest_countries":  REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
    }

    for c in COUNTRIES:
        print(f"\n▶ {c['country']} ({c['iso2']})")
        out["countries"].append(build_country(c["country"], c["iso2"], prev))
        time.sleep(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")


if __name__ == "__main__":
    main()
