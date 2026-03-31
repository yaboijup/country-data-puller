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

# Countries where IPU data is not applicable
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
    """Load the entire previous snapshot dict (not just countries)."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
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

# ── BIWEEKLY REFRESH LOGIC ─────────────────────────────────────────────────────
# Fires on every-other-Tuesday. The previous snapshot stores the last full-sweep
# date so we can compare across runs. "Every other Tuesday" = Tuesday AND ≥13
# days since the last full sweep (13-day floor allows for 1-day scheduling drift).

def _is_biweekly_tuesday(prev_full_snapshot: Dict[str, Any]) -> bool:
    """Return True if today is Tuesday and ≥13 days since the last full sweep."""
    today = datetime.now(timezone.utc)
    if today.weekday() != 1:   # 0=Mon, 1=Tue …
        return False
    last_sweep = prev_full_snapshot.get("lastFullSweepDate")
    if not last_sweep:
        return True            # First ever run — treat as biweekly
    try:
        last_dt = datetime.fromisoformat(last_sweep.replace("Z", "+00:00"))
        days_since = (today - last_dt).days
        return days_since >= 13
    except (ValueError, AttributeError):
        return True


# ── ELECTION WATCH ─────────────────────────────────────────────────────────────
# A country enters "election watch" when any election date in its snapshot is
# within the next 3 calendar days (inclusive of today). Once open, the watch
# stays active (electionWatchActive: true) until Claude reports the election as
# resolved. The watch also fires if electionWatchActive is already true in the
# previous snapshot (carries forward until explicitly cleared by Claude).

def _election_watch_active(prev: Optional[Dict]) -> Tuple[bool, str]:
    """
    Returns (is_active, reason).

    Triggers if:
      (a) electionWatchActive is already True in the previous snapshot, OR
      (b) Any nextElection date is within 3 calendar days from today.
    """
    if not prev:
        return False, ""

    # (a) Carry-forward: still active from a previous run
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
                # Also watch up to 14 days after (result period) when watch was
                # already open — handled by carry-forward above, but belt+suspenders
                if -14 <= days_until < 0 and prev.get("elections", {}).get("electionWatchActive"):
                    return True, f"election_result_window ({d})"
            except (ValueError, AttributeError):
                continue

    return False, ""


# ── CHANGE-IN-POWER SENTINEL ───────────────────────────────────────────────────
# Fetches the sentinel JSON feed, compares article IDs against previously-seen
# IDs stored in the snapshot, and asks Claude (no web search) to evaluate only
# new articles. Affected countries get a changeInPowerAlert flag in their entry
# so the next scheduled hard refresh picks it up.

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
    """
    Fetches the sentinel JSON, evaluates new articles with Claude (no web search),
    and returns a dict mapping iso2 → alert_string for any flagged countries.
    Also returns the updated set of seen article IDs (stored back to snapshot).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    print("\n── Change-in-Power Sentinel ──────────────────────────────────────────")

    raw = req_json(CHANGE_IN_POWER_URL, label="change-in-power sentinel feed")
    if not raw:
        print("  [SENTINEL] Failed to fetch sentinel feed — skipping")
        return {}

    # The feed may be a list of articles or a dict with an articles key
    articles: List[Dict] = []
    if isinstance(raw, list):
        articles = raw
    elif isinstance(raw, dict):
        articles = (raw.get("articles") or raw.get("items") or raw.get("data")
                    or raw.get("results") or [])
        if not articles:
            # Treat the dict itself as a single-entry list if it has id/title
            if raw.get("id") or raw.get("title"):
                articles = [raw]

    if not articles:
        print("  [SENTINEL] No articles found in feed")
        return {}

    print(f"  [SENTINEL] {len(articles)} article(s) in feed")

    # Load previously-seen IDs so we only evaluate new articles
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
                "model":      CLAUDE_MODEL,
                "max_tokens": 1000,
                "system":     SENTINEL_SYSTEM,
                # No tools — Claude reads the JSON only, no web search
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
    """Merge current article IDs into the seen-IDs list for storage."""
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

    if _ipu_parliament_map:
        sample_iso2 = next(iter(_ipu_parliament_map))
        sample = _ipu_parliament_map[sample_iso2]
        print(f"  [IPU] Sample record ({sample_iso2}) keys: {list(sample.keys())[:15]}")

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


def fetch_ipu_elections(iso2: str) -> Dict[str, Any]:
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


# ── CLAUDE API ────────────────────────────────────────────────────────────────

import os

ANTHROPIC_API_URL    = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL         = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS    = 4000
CLAUDE_FORCE_REFRESH = os.environ.get("CLAUDE_FORCE_REFRESH", "").strip() == "1"

# ── CLAUDE SYSTEM PROMPT ──────────────────────────────────────────────────────

CLAUDE_SYSTEM = """\
You are a political data analyst maintaining a structured JSON dataset of country \
leadership, legislature control, election data, and party profiles. You will receive:
  - The country name and ISO2 code
  - Whatever the free scrapers found (Wikipedia leader names, IPU/EG election dates)
  - The previous snapshot for this country (may be months old)
  - Today's date
  - Whether this is an election-watch call (electionWatchActive in previous snapshot)

Return ONLY a single valid JSON object — no markdown, no explanation — with these keys:

{
  "headOfState":          {"name": str, "partyOrGroup": str},
  "headOfGovernment":     {"name": str, "partyOrGroup": str},
  "politicalSystem":      [str, ...],
  "legislature":          [{"name": str, "inControl": str}, ...],
  "competitiveElections": bool,
  "nonCompetitiveReason": str | null,
  "electionsSuspended":   bool,
  "suspensionReason":     str | null,
  "electionWatchActive":  bool,
  "electionWatchReason":  str | null,
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
   it as upcoming. Do not invent winners, vote shares, or coalition outcomes.

3. POST-ELECTION COALITION TALKS: If parliament just voted and coalition talks are
   ongoing, notes must say "Coalition talks ongoing as of [date]". Do not state a
   PM has been confirmed until that is factual.

4. For acting/interim leaders, use the person actually exercising power today.

5. For one-party states: headOfState = formal president. Note supreme power-holder
   in dataAvailabilityNotes.

6. dataAvailabilityNotes is mandatory for disputed legitimacy, parallel governments,
   suspended elections, acting leaders, one-party context, or any nuance needed.

7. election notes: factual, 1-2 sentences. Past-tense for completed events.
   Future-tense for scheduled events.

8. legislature[].inControl: reflect post-election reality. If coalition talks are
   ongoing, say "Pending coalition formation (election [date])".

9. runoffDate: only populate if a specific runoff date is scheduled. Otherwise use
   runoffCondition only.

10. ELECTION WATCH — electionWatchActive field:
    - Set to TRUE if: (a) an election date is within the next 3 days, OR (b) an
      election has occurred but no official winner/government has been confirmed yet
      (counting ongoing, coalition talks, runoff pending, result disputed, etc.).
    - Set to FALSE if: a clear winner or new government has been officially confirmed
      and power has transferred (or will transfer on a known date). When setting to
      false, populate electionWatchReason with a brief explanation.
    - During election-watch calls, use web search aggressively to verify the current
      status. Do NOT rely on training data for recent election outcomes.

11. partyProfileUpdates: ONLY include parties that are newly in power (executive or
    legislature) OR whose profile does not yet exist in the snapshot. Do not
    re-submit profiles for parties already profiled and still in power.
    - politicalOrientation: one of "Far-Left", "Left", "Centre-Left", "Centre",
      "Centre-Right", "Right", "Far-Right", "Authoritarian", "Theocratic",
      "Nationalist", "Communist", "Mixed/Unclear"
    - ideologyTags: short list of tags, e.g. ["social democracy", "Keynesian economics"]
    - keyPlatforms: 3-5 bullet points describing their main policy positions

12. Return ONLY the JSON object — no markdown fences, no preamble, no explanation.\
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


def _should_call_claude(
    iso2: str,
    wiki_names: Dict,
    ipu: Dict,
    eg: Dict,
    prev: Optional[Dict],
    biweekly_tuesday: bool,
    sentinel_alerts: Dict[str, str],
) -> Tuple[bool, str]:
    """
    Decide whether to call Claude for this country.
    Returns (should_call, reason).

    Priority order:
      1. CLAUDE_FORCE_REFRESH env var
      2. No previous snapshot (first run)
      3. Election watch active (daily hard search until winner confirmed)
      4. Sentinel flagged this country in the current run
      5. Wikipedia returned a different leader name
      6. IPU or EG returned a new election date not in snapshot
      7. Biweekly Tuesday refresh (≥13 days since last full sweep)
    """
    if CLAUDE_FORCE_REFRESH:
        return True, "forced_refresh"

    if not prev:
        return True, "first_run"

    # Priority 3: Election watch
    watch_active, watch_reason = _election_watch_active(prev)
    if watch_active:
        return True, f"election_watch ({watch_reason})"

    # Priority 4: Sentinel alert for this country
    if iso2.upper() in sentinel_alerts:
        return True, f"sentinel_alert: {sentinel_alerts[iso2.upper()]}"

    # Priority 5: Wikipedia name changed
    prev_hos = ((prev.get("executive") or {}).get("headOfState") or {}).get("name")
    prev_hog = ((prev.get("executive") or {}).get("headOfGovernment") or {}).get("name")
    wiki_hos = _clean_wiki(wiki_names.get("hosName"))
    wiki_hog = _clean_wiki(wiki_names.get("hogName"))
    if wiki_hos and wiki_hos != prev_hos:
        return True, f"executive_name_changed ({prev_hos!r} → {wiki_hos!r})"
    if wiki_hog and wiki_hog != prev_hog:
        return True, f"executive_name_changed ({prev_hog!r} → {wiki_hog!r})"

    # Priority 6: IPU/EG new date
    prev_leg_next = ((prev.get("elections") or {}).get("legislative") or {}).get("nextElection") or {}
    ipu_next = ipu.get("nextDate")
    eg_next  = eg.get("nextDate")
    if ipu_next and ipu_next != prev_leg_next.get("date"):
        return True, f"ipu_date_changed ({ipu_next})"
    if eg_next and eg_next != prev_leg_next.get("date"):
        return True, f"eg_date_changed ({eg_next})"

    # Priority 7: Biweekly Tuesday
    days_old = _days_since_claude(prev)
    if biweekly_tuesday and days_old >= 13:
        return True, f"biweekly_tuesday_refresh (last_update_{days_old}d_ago)"

    return False, ""


# ── CLAUDE API CALL ───────────────────────────────────────────────────────────

def _call_claude(country_name: str, iso2: str,
                 wiki_names: Dict, ipu: Dict, eg: Dict,
                 prev: Optional[Dict], trigger_reason: str) -> Optional[Dict]:
    """
    Call Claude with live web search. Handles the multi-turn tool-use loop.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    today = datetime.now(timezone.utc).date().isoformat()

    # Include electionWatchActive in context so Claude knows to be thorough
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
            "executive":          prev.get("executive")       if prev else None,
            "politicalSystem":    prev.get("politicalSystem") if prev else None,
            "legislature":        prev.get("legislature")     if prev else None,
            "elections":          prev.get("elections")       if prev else None,
            "partyProfiles":      prev.get("partyProfiles")   if prev else None,
            "lastClaudeUpdate":   prev.get("lastClaudeUpdate") if prev else None,
        } if prev else None,
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
                    "model":      CLAUDE_MODEL,
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
    """
    Merge incoming partyProfileUpdates from Claude into the existing profiles.
    Existing profiles are never deleted — only added or updated.
    """
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
    """
    Turn Claude's response dict into:
      (executive_block, legislature_block, elections_block, party_profiles)
    """

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

    # Election watch: use what Claude returned, defaulting to False if absent
    election_watch_active = bool(cl.get("electionWatchActive", False))
    election_watch_reason = cl.get("electionWatchReason")

    elections_block = {
        "competitiveElections": cl.get("competitiveElections", True),
        "nonCompetitiveReason": cl.get("nonCompetitiveReason"),
        "electionsSuspended":   cl.get("electionsSuspended", False),
        "suspensionReason":     cl.get("suspensionReason"),
        "electionToday":        election_today,
        "electionWatchActive":  election_watch_active,
        "electionWatchReason":  election_watch_reason,
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

    # Merge party profiles
    party_profiles = _merge_party_profiles(prev_profiles, cl.get("partyProfileUpdates"))

    return executive_block, legislature_block, elections_block, party_profiles


# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def build_country(
    name: str,
    iso2: str,
    prev_by_iso2: Dict[str, Any],
    biweekly_tuesday: bool,
    sentinel_alerts: Dict[str, str],
) -> Dict[str, Any]:
    prev = prev_by_iso2.get(iso2)
    today_str = datetime.now(timezone.utc).date().isoformat()

    # ── Free scrapers ──────────────────────────────────────────────────────────
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

    # ── Claude trigger decision ────────────────────────────────────────────────
    should_call, trigger_reason = _should_call_claude(
        iso2, wiki, ipu, eg, prev, biweekly_tuesday, sentinel_alerts,
    )

    if should_call:
        print(f"  [{iso2}] 🤖 Claude triggered: {trigger_reason}")
        cl = _call_claude(name, iso2, wiki, ipu, eg, prev, trigger_reason)
    else:
        cl = None
        print(f"  [{iso2}] ✓  Claude skipped (last updated {_days_since_claude(prev)}d ago)")

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
            "competitiveElections": None, "nonCompetitiveReason": None,
            "electionsSuspended": False, "suspensionReason": None,
            "electionToday": False,
            "electionWatchActive": False, "electionWatchReason": None,
            "legislative": {"lastElection": None, "nextElection": None, "source": "unknown"},
            "executive":   {"lastElection": None, "nextElection": None, "source": "unknown"},
        }
        party_profiles    = None
        pol_sys           = {"values": ["unknown"], "source": "unknown"}
        data_avail_note   = None
        last_claude_update = None

    # Attach sentinel alert if present (cleared on next hard refresh)
    sentinel_alert = sentinel_alerts.get(iso2.upper())

    # Data availability notes
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
        "lastClaudeUpdate":    last_claude_update,
    }

    if sentinel_alert:
        entry["changeInPowerAlert"] = {
            "alert":      sentinel_alert,
            "detectedAt": iso_z(now_utc()),
            "resolved":   False,
        }
    elif prev and prev.get("changeInPowerAlert") and not prev["changeInPowerAlert"].get("resolved"):
        # Carry forward unresolved alert until a Claude refresh clears it
        if cl:
            # Claude just ran — mark as resolved (Claude's refresh supersedes)
            entry["changeInPowerAlert"] = dict(prev["changeInPowerAlert"])
            entry["changeInPowerAlert"]["resolved"] = True
            entry["changeInPowerAlert"]["resolvedAt"] = iso_z(now_utc())
        else:
            entry["changeInPowerAlert"] = prev["changeInPowerAlert"]

    return entry


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("docs") / "countries_snapshot.json"

    prev_full = load_full_previous_snapshot(out_path)
    prev_by_iso2 = {c["iso2"]: c for c in prev_full.get("countries", []) if c.get("iso2")}
    print(f"=== Starting build. Previous snapshot: {len(prev_by_iso2)} countries cached ===")

    # ── Biweekly Tuesday check ─────────────────────────────────────────────────
    biweekly_tuesday = _is_biweekly_tuesday(prev_full)
    if biweekly_tuesday:
        print(f"  [SCHEDULE] 📅 Biweekly Tuesday refresh triggered")
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
        print(f"  [SCHEDULE] Today is {today_wd}, {days_since_sweep}d since last sweep — biweekly NOT triggered")

    # ── Change-in-power sentinel ───────────────────────────────────────────────
    sentinel_alerts = run_change_in_power_sentinel(prev_full)

    # Update seen IDs (will be saved into the output snapshot)
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

    # ── Pre-load shared caches ─────────────────────────────────────────────────
    _load_ipu_parliament_map()
    _load_electionguide_cache()
    _load_wiki_exec_cache()

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
    }

    for c in COUNTRIES:
        print(f"\n▶ {c['country']} ({c['iso2']})")
        country_data = build_country(
            c["country"], c["iso2"], prev_by_iso2,
            biweekly_tuesday, sentinel_alerts,
        )
        out["countries"].append(country_data)
        time.sleep(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")


if __name__ == "__main__":
    main()
