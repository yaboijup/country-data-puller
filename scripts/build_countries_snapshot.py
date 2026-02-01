"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

Adds:
- Wikipedia English summary description per country
- Political freedom rating (Freedom House PR+CL 0–100) via OWID grapher CSVs
- Next national election date (best-effort):
  1) Upcoming election items in Wikidata (jurisdiction + future P585)
  2) If missing: estimate next election from last national election date + term length of office (P2097)
     where office contested (P541) is available on the last election
  3) If political system strongly suggests no meaningful national elections: hasElections=false

- Better leader resolution (HoS + HoG; if same person, mark samePerson=true)
- Political system: single label
- Executive controller: leader party (Wikidata)
- Legislature controller (best-effort): winner of most recent national legislative election (Wikidata P1346)

News:
- 5 unique English-language news stories per country (GDELT Doc API):
  - clustered to avoid near-duplicates
  - includes title, publishedAt (ISO), source, url
  - query uses the English country spelling (your country name / aliases), and sourcelang=English

Removed entirely:
- Google Trends top searches
- US spike in search interest
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------------------- CONFIG ----------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5

NEWS_WINDOW_DAYS = 5
TOP_NEWS_N = 5

# ENFORCE: only keep English-script headlines for news
ENGLISH_NEWS_ONLY = True

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

# Wikipedia REST summary (English)
WIKI_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"

# Freedom House (via Our World in Data grapher CSVs, updated annually)
OWID_FH_PR_CSV = "https://ourworldindata.org/grapher/political-rights-score-fh.csv"
OWID_FH_CL_CSV = "https://ourworldindata.org/grapher/civil-liberties-score-fh.csv"


# ---------------------------- COUNTRY LIST ----------------------------

COUNTRIES: List[Dict[str, str]] = [
    {"country": "Ukraine", "iso2": "UA"},
    {"country": "Russia", "iso2": "RU"},
    {"country": "India", "iso2": "IN"},
    {"country": "Pakistan", "iso2": "PK"},
    {"country": "China", "iso2": "CN"},
    {"country": "United Kingdom", "iso2": "GB"},
    {"country": "Germany", "iso2": "DE"},
    {"country": "UAE", "iso2": "AE"},
    {"country": "Saudi Arabia", "iso2": "SA"},
    {"country": "Israel", "iso2": "IL"},
    {"country": "Palestine", "iso2": "PS"},
    {"country": "Mexico", "iso2": "MX"},
    {"country": "Brazil", "iso2": "BR"},
    {"country": "Canada", "iso2": "CA"},
    {"country": "Nigeria", "iso2": "NG"},
    {"country": "Japan", "iso2": "JP"},
    {"country": "Iran", "iso2": "IR"},
    {"country": "Syria", "iso2": "SY"},
    {"country": "France", "iso2": "FR"},
    {"country": "Turkey", "iso2": "TR"},
    {"country": "Venezuela", "iso2": "VE"},
    {"country": "Vietnam", "iso2": "VN"},
    {"country": "Taiwan", "iso2": "TW"},
    {"country": "South Korea", "iso2": "KR"},
    {"country": "North Korea", "iso2": "KP"},
    {"country": "Indonesia", "iso2": "ID"},
    {"country": "Myanmar", "iso2": "MM"},
    {"country": "Armenia", "iso2": "AM"},
    {"country": "Azerbaijan", "iso2": "AZ"},
    {"country": "Morocco", "iso2": "MA"},
    {"country": "Somalia", "iso2": "SO"},
    {"country": "Yemen", "iso2": "YE"},
    {"country": "Libya", "iso2": "LY"},
    {"country": "Egypt", "iso2": "EG"},
    {"country": "Algeria", "iso2": "DZ"},
    {"country": "Argentina", "iso2": "AR"},
    {"country": "Chile", "iso2": "CL"},
    {"country": "Peru", "iso2": "PE"},
    {"country": "Cuba", "iso2": "CU"},
    {"country": "Colombia", "iso2": "CO"},
    {"country": "Panama", "iso2": "PA"},
    {"country": "El Salvador", "iso2": "SV"},
    {"country": "Denmark", "iso2": "DK"},
    {"country": "Sudan", "iso2": "SD"},
]

QUERY_ALIASES: Dict[str, List[str]] = {
    "UAE": ["United Arab Emirates", "UAE"],
    "United Kingdom": ["United Kingdom", "UK", "Britain"],
    "Palestine": ["Palestine", "Palestinian Territories"],
    "Taiwan": ["Taiwan", "Republic of China"],
    "North Korea": ["North Korea", "DPRK"],
    "South Korea": ["South Korea", "Republic of Korea"],
}


# ---------------------------- BASIC HELPERS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_text(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[str]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
        except requests.RequestException:
            pass
        _sleep_backoff(attempt)
    return None

def req_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[dict]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        _sleep_backoff(attempt)
    return None

def safe_get(d: Any, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# ---------------------------- LANGUAGE / ENGLISH FILTER ----------------------------

def is_english_script(text: str) -> bool:
    if not text:
        return False
    # exclude common non-latin scripts
    if re.search(r"[А-Яа-я]", text):
        return False
    if re.search(r"[\u0600-\u06FF]", text):
        return False
    if re.search(r"[\u4e00-\u9fff]", text):
        return False
    if re.search(r"[\u3040-\u30ff]", text):
        return False
    if re.search(r"[\uac00-\ud7af]", text):
        return False
    return bool(re.search(r"[A-Za-z]", text))


# ---------------------------- WIKIDATA ----------------------------

def wikidata_sparql(query: str) -> Optional[dict]:
    return req_json(
        WIKIDATA_SPARQL,
        params={"format": "json", "query": query},
        headers={"Accept": "application/sparql-results+json"},
    )

def _wd_val(b: dict, key: str) -> Optional[str]:
    v = b.get(key)
    if not v:
        return None
    return v.get("value")

def get_wikidata_country_qid_by_iso2(iso2: str) -> Optional[str]:
    q = f"""
    SELECT ?country WHERE {{
      ?country wdt:P297 "{iso2}" .
    }} LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return None
    uri = _wd_val(bindings[0], "country")
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]  # Qxxx


def get_political_system_single(country_qid: str) -> str:
    """
    Return ONE political system label (English).
    """
    q = f"""
    SELECT ?polsysLabel WHERE {{
      wd:{country_qid} p:P122 ?stmt .
      ?stmt ps:P122 ?polsys .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return "unknown"
    return _wd_val(bindings[0], "polsysLabel") or "unknown"


def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    """
    English labels:
    - head of state (P35) + party (P102)
    - head of government (P6) + party (P102)
    - legislature bodies (P194) list (names)
    """
    q = f"""
    SELECT
      ?hosLabel ?hosPartyLabel
      ?hogLabel ?hogPartyLabel
      ?legLabel
    WHERE {{
      OPTIONAL {{
        wd:{country_qid} wdt:P35 ?hos .
        OPTIONAL {{ ?hos wdt:P102 ?hosParty . }}
      }}
      OPTIONAL {{
        wd:{country_qid} wdt:P6 ?hog .
        OPTIONAL {{ ?hog wdt:P102 ?hogParty . }}
      }}
      OPTIONAL {{
        wd:{country_qid} wdt:P194 ?leg .
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 200
    """

    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    hos_name = None
    hos_party = None
    hog_name = None
    hog_party = None
    legislatures = set()

    for b in bindings:
        if not hos_name:
            hos_name = _wd_val(b, "hosLabel")
        if not hos_party:
            hos_party = _wd_val(b, "hosPartyLabel")
        if not hog_name:
            hog_name = _wd_val(b, "hogLabel")
        if not hog_party:
            hog_party = _wd_val(b, "hogPartyLabel")
        leg = _wd_val(b, "legLabel")
        if leg:
            legislatures.add(leg)

    same_person = bool(hos_name and hog_name and hos_name == hog_name)

    leaders = []
    if hos_name:
        leaders.append({"name": hos_name, "title": "Head of State", "party": hos_party})
    if hog_name:
        leaders.append({"name": hog_name, "title": "Head of Government", "party": hog_party})

    leader_notes = "Head of State and Head of Government are the same person." if same_person else ""

    return {
        "leaders": leaders,
        "leadersSamePerson": same_person,
        "leaderNotes": leader_notes,
        "executiveControllerParty": hog_party or hos_party or "unknown",
        "legislatureBodies": sorted(legislatures),
    }


# ---------------------------- ELECTIONS ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def infer_no_elections_from_system(political_system: str) -> Optional[bool]:
    """
    Heuristic: if system label strongly suggests no meaningful national elections.
    Return:
      - False => likely no meaningful national elections
      - None  => unsure
    """
    s = (political_system or "").lower()
    strong_no = [
        "absolute monarchy",
        "military dictatorship",
        "military junta",
        "junta",
        "one-party state",
        "one party state",
        "totalitarian",
    ]
    if any(t in s for t in strong_no):
        return False
    return None

def get_next_national_election_upcoming(country_qid: str) -> Dict[str, Any]:
    """
    1) Upcoming election item for this jurisdiction (P1001) with date (P585) in the future.
    """
    today = _today_yyyymmdd()
    q = f"""
    SELECT ?e ?eLabel ?date ?typeLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date >= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{
        wd:Q152203       # general election
        wd:Q1079032      # parliamentary election
        wd:Q104203       # legislative election
        wd:Q159821       # presidential election
        wd:Q40231        # election (broad)
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {
            "hasElections": "unknown",
            "date": None,
            "type": None,
            "name": None,
            "method": "wikidata_upcoming",
            "notes": "No upcoming national election item found in Wikidata (common data gap).",
        }

    b = bindings[0]
    return {
        "hasElections": True,
        "date": _wd_val(b, "date"),
        "type": _wd_val(b, "typeLabel"),
        "name": _wd_val(b, "eLabel"),
        "method": "wikidata_upcoming",
        "notes": "From Wikidata upcoming election items (jurisdiction + future date).",
    }

def get_last_national_election_with_term_hint(country_qid: str) -> Dict[str, Any]:
    """
    Find most recent national election (general/parliamentary/legislative/presidential)
    and try to extract:
      - office contested (P541)
      - term length of position (P2097) from that office
    """
    today = _today_yyyymmdd()
    q = f"""
    SELECT ?e ?eLabel ?date ?typeLabel ?office ?officeLabel ?termYears WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{
        wd:Q152203       # general election
        wd:Q1079032      # parliamentary election
        wd:Q104203       # legislative election
        wd:Q159821       # presidential election
      }}

      OPTIONAL {{
        ?e wdt:P541 ?office .
        OPTIONAL {{ ?office wdt:P2097 ?termYears . }}
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {
            "found": False,
            "electionName": None,
            "electionDate": None,
            "electionType": None,
            "officeContested": None,
            "termYears": None,
        }

    b = bindings[0]
    term = _wd_val(b, "termYears")
    term_years = None
    if term:
        try:
            term_years = float(term)
        except ValueError:
            term_years = None

    return {
        "found": True,
        "electionName": _wd_val(b, "eLabel"),
        "electionDate": _wd_val(b, "date"),
        "electionType": _wd_val(b, "typeLabel"),
        "officeContested": _wd_val(b, "officeLabel"),
        "termYears": term_years,
    }

def estimate_next_election(country_qid: str) -> Dict[str, Any]:
    """
    2) Estimate: last election date + termYears (if office contested + term length exists).
    """
    last = get_last_national_election_with_term_hint(country_qid)
    if not last.get("found") or not last.get("electionDate"):
        return {
            "hasElections": "unknown",
            "date": None,
            "type": None,
            "name": None,
            "method": "estimate_from_last_plus_term",
            "notes": "No prior national election found to estimate from.",
        }

    term_years = last.get("termYears")
    if not term_years:
        return {
            "hasElections": True,
            "date": None,
            "type": last.get("electionType"),
            "name": None,
            "method": "estimate_from_last_plus_term",
            "notes": "Found last election, but could not find term length of the contested office (P2097), so no estimate produced.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
            },
        }

    try:
        # Wikidata returns xsd:dateTime like 2024-07-04T00:00:00Z
        dt = datetime.fromisoformat(str(last["electionDate"]).replace("Z", "+00:00"))
        est = dt.replace(year=dt.year + int(round(term_years)))
        # If termYears was non-integer (rare), approximate by days:
        if abs(term_years - round(term_years)) > 1e-6:
            est = dt + timedelta(days=int(term_years * 365.25))
        return {
            "hasElections": True,
            "date": iso_z(est),
            "type": last.get("electionType"),
            "name": None,
            "method": "estimate_from_last_plus_term",
            "notes": "Estimated as last national election date + term length of office contested (Wikidata P541 + P2097). Treat as an estimate.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
                "termYears": term_years,
            },
        }
    except Exception:
        return {
            "hasElections": True,
            "date": None,
            "type": last.get("electionType"),
            "name": None,
            "method": "estimate_from_last_plus_term",
            "notes": "Failed to compute estimate from last election date + term length.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
                "termYears": term_years,
            },
        }

def get_next_national_election(country_qid: str, political_system: str) -> Dict[str, Any]:
    """
    Combined resolver:
      1) upcoming item
      2) estimate from last+term
      3) infer no elections (strong heuristic)
    """
    upcoming = get_next_national_election_upcoming(country_qid)
    if upcoming.get("hasElections") is True and upcoming.get("date"):
        return upcoming

    inferred = infer_no_elections_from_system(political_system)
    if inferred is False:
        # Say "no meaningful national elections"
        return {
            "hasElections": False,
            "date": None,
            "type": None,
            "name": None,
            "method": "political_system_heuristic",
            "notes": "Political system label suggests no meaningful national elections.",
        }

    est = estimate_next_election(country_qid)
    # If estimate yields a date, prefer it over unknown
    if est.get("date"):
        return est

    # otherwise return upcoming (unknown) but keep notes
    return upcoming


def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    Approximate legislature control using winner (P1346) of the most recent legislative/parliamentary/general election.
    """
    today = _today_yyyymmdd()

    q = f"""
    SELECT ?e ?eLabel ?date ?winnerLabel ?typeLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{
        wd:Q152203       # general election
        wd:Q1079032      # parliamentary election
        wd:Q104203       # legislative election
      }}

      OPTIONAL {{ ?e wdt:P1346 ?winner . }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"winner": "unknown", "electionName": None, "electionDate": None, "notes": "No prior legislative election item found in Wikidata."}

    b = bindings[0]
    return {
        "winner": _wd_val(b, "winnerLabel") or "unknown",
        "electionName": _wd_val(b, "eLabel"),
        "electionDate": _wd_val(b, "date"),
        "notes": "Legislature control approximated from last national legislative election winner (Wikidata P1346).",
    }


# ---------------------------- WIKIPEDIA DESCRIPTION ----------------------------

def wikipedia_summary(country_name: str) -> Dict[str, Any]:
    title = country_name.replace(" ", "_")
    data = req_json(WIKI_SUMMARY_API + title)
    if not isinstance(data, dict):
        return {"summary": None, "source": "wikipedia", "url": None}

    extract = (data.get("extract") or "").strip()
    url = safe_get(data, "content_urls", "desktop", "page", default=None)

    if not extract and country_name in QUERY_ALIASES:
        alt = QUERY_ALIASES[country_name][0].replace(" ", "_")
        data2 = req_json(WIKI_SUMMARY_API + alt)
        extract = (data2.get("extract") or "").strip() if isinstance(data2, dict) else ""
        url = safe_get(data2, "content_urls", "desktop", "page", default=url) if isinstance(data2, dict) else url

    return {"summary": extract or None, "source": "wikipedia", "url": url}


# ---------------------------- FREEDOM HOUSE (OWID CSV) ----------------------------

def _parse_owid_csv(url: str) -> Dict[str, Dict[int, float]]:
    text = req_text(url)
    if not text:
        return {}
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    out: Dict[str, Dict[int, float]] = {}
    for row in reader:
        entity = (row.get("Entity") or row.get("entity") or "").strip()
        year_str = (row.get("Year") or row.get("year") or "").strip()
        val_str = (
            row.get("political-rights-score-fh")
            or row.get("civil-liberties-score-fh")
            or row.get("Value")
            or row.get("value")
            or ""
        ).strip()

        if not entity or not year_str or not val_str:
            continue
        try:
            year = int(year_str)
            val = float(val_str)
        except ValueError:
            continue
        out.setdefault(entity, {})[year] = val
    return out

def load_freedom_house_tables() -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
    pr = _parse_owid_csv(OWID_FH_PR_CSV)
    cl = _parse_owid_csv(OWID_FH_CL_CSV)
    return pr, cl

def freedom_house_rating(country_name: str, pr_table: Dict[str, Dict[int, float]], cl_table: Dict[str, Dict[int, float]]) -> Dict[str, Any]:
    candidates = [country_name]
    if country_name in QUERY_ALIASES:
        candidates = QUERY_ALIASES[country_name] + candidates

    best_entity = None
    best_year = None
    best_pr = None
    best_cl = None

    for ent in candidates:
        pr_years = pr_table.get(ent, {})
        cl_years = cl_table.get(ent, {})
        if not pr_years or not cl_years:
            continue
        common_years = sorted(set(pr_years.keys()) & set(cl_years.keys()))
        if not common_years:
            continue
        y = common_years[-1]
        best_entity = ent
        best_year = y
        best_pr = pr_years[y]
        best_cl = cl_years[y]
        break

    if best_year is None or best_pr is None or best_cl is None:
        return {"score": None, "status": "unknown", "year": None, "notes": "Freedom House score not found in OWID tables."}

    total = float(best_pr) + float(best_cl)
    if total >= 70:
        status = "Free"
    elif total >= 35:
        status = "Partly Free"
    else:
        status = "Not Free"

    return {
        "score": round(total, 1),
        "status": status,
        "year": best_year,
        "components": {
            "politicalRights": round(float(best_pr), 1),
            "civilLiberties": round(float(best_cl), 1),
        },
        "sourceEntityName": best_entity,
        "notes": "Computed as PR(0–40)+CL(0–60) using OWID grapher (Freedom House).",
    }


# ---------------------------- NEWS (GDELT) ----------------------------

def _country_queries(country_name: str) -> List[str]:
    return QUERY_ALIASES.get(country_name, [country_name])

def _gdelt_dt_to_iso(s: str) -> Optional[str]:
    """
    GDELT doc api often returns seendate like '20260131142000'
    or sometimes ISO-ish. Normalize to ISO Z when possible.
    """
    if not s:
        return None
    s = str(s).strip()
    # already ISO-ish
    if "T" in s and ("Z" in s or "+" in s):
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return iso_z(dt)
        except Exception:
            return s
    # yyyymmddhhmmss
    if re.fullmatch(r"\d{14}", s):
        try:
            dt = datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            return iso_z(dt)
        except Exception:
            return None
    # yyyymmdd
    if re.fullmatch(r"\d{8}", s):
        try:
            dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
            return iso_z(dt)
        except Exception:
            return None
    return s

def gdelt_recent_articles(country_name: str, max_pull: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch English-language stories mentioning the country in the last NEWS_WINDOW_DAYS.
    Enforces:
      - sourcelang=English
      - title must be Latin/English script (optional)
      - query uses English country name spelling (aliases)
    """
    start = (now_utc() - timedelta(days=NEWS_WINDOW_DAYS)).strftime("%Y%m%d%H%M%S")
    end = now_utc().strftime("%Y%m%d%H%M%S")

    queries = _country_queries(country_name)
    q = " OR ".join([f'"{x}"' for x in queries])

    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_pull),
        "startdatetime": start,
        "enddatetime": end,
        "sourcelang": "English",
        "sort": "HybridRel",
    }

    data = req_json(GDELT_DOC, params=params)
    arts = safe_get(data, "articles", default=[]) if isinstance(data, dict) else []

    out: List[Dict[str, Any]] = []
    seen = set()

    for a in arts:
        title = (a.get("title") or "").strip()
        url = (a.get("url") or "").strip()
        dt_raw = (a.get("seendate") or a.get("date") or "").strip()
        source = (a.get("sourceCommonName") or a.get("source") or "").strip()

        if not title or not url:
            continue
        if ENGLISH_NEWS_ONLY and not is_english_script(title):
            continue

        key = url.lower()
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "title": title,
            "url": url,
            "source": (source[:120] if source else None),
            "publishedAt": _gdelt_dt_to_iso(dt_raw),
        })

    return out

def _topic_key(title: str, country_name: str) -> str:
    """
    Very light clustering so we get 5 "different stories", not 5 rewrites.
    """
    t = title.lower()

    # remove country words / aliases
    for alias in _country_queries(country_name):
        t = t.replace(alias.lower(), " ")

    # remove punctuation-ish
    t = re.sub(r"[^a-z0-9\s]", " ", t)

    stop = {
        "the","a","an","and","or","to","of","in","on","for","with","as","at","by","from",
        "after","before","amid","over","into","about","against","between","during",
        "says","said","say","report","reports","reported","update","live",
        "new","latest","breaking","why","how","what","when",
        "government","president","prime","minister","parliament","election",
        "country","states","state",
    }

    toks = [x for x in t.split() if len(x) >= 4 and x not in stop]
    # key from top-ish tokens in order
    return " ".join(toks[:6]) if toks else title.lower()[:60]

def gdelt_top_unique_topics(country_name: str, n: int = TOP_NEWS_N) -> List[Dict[str, Any]]:
    arts = gdelt_recent_articles(country_name, max_pull=240)

    chosen: List[Dict[str, Any]] = []
    used_topics = set()

    for a in arts:
        key = _topic_key(a["title"], country_name)
        if key in used_topics:
            continue
        used_topics.add(key)
        chosen.append(a)
        if len(chosen) >= n:
            break

    return chosen


# ---------------------------- BUILD ----------------------------

def build_country(
    country_name: str,
    iso2: str,
    pr_table: Dict[str, Dict[int, float]],
    cl_table: Dict[str, Dict[int, float]],
) -> Dict[str, Any]:
    warnings: List[str] = []
    confidence = 1.0

    # Description
    desc = wikipedia_summary(country_name)
    if not desc.get("summary"):
        warnings.append("Wikipedia summary missing/unavailable for this title.")
        confidence -= 0.05

    # Government & system
    political_system = "unknown"
    gov = {
        "politicalSystem": "unknown",
        "leaders": [],
        "leadersSamePerson": False,
        "leaderNotes": "",
        "executiveControllerParty": "unknown",
        "legislatureBodies": [],
    }

    qid = get_wikidata_country_qid_by_iso2(iso2)
    if not qid:
        warnings.append("Wikidata entity not found via ISO2; government/elections fields missing.")
        confidence -= 0.25
    else:
        try:
            political_system = get_political_system_single(qid)
            gov = get_government_snapshot(qid)
            gov["politicalSystem"] = political_system

            if not gov.get("leaders"):
                warnings.append("Leader fields missing (Wikidata gaps).")
                confidence -= 0.10

            if political_system == "unknown":
                warnings.append("Political system missing from Wikidata.")
                confidence -= 0.05
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.20

    # Elections
    next_elec = {"hasElections": "unknown", "date": None, "type": None, "name": None, "method": None, "notes": "unknown"}
    leg_control = {"winner": "unknown", "electionName": None, "electionDate": None, "notes": "unknown"}

    if qid:
        try:
            next_elec = get_next_national_election(qid, political_system)

            if next_elec.get("hasElections") is True and not next_elec.get("date"):
                warnings.append("Next election date unavailable (Wikidata gaps and no term-length estimate).")
                confidence -= 0.06

            leg_control = get_last_legislative_election_winner(qid)
            if leg_control.get("winner") == "unknown":
                warnings.append("Legislative control winner not found (Wikidata election winner missing).")
                confidence -= 0.05
        except Exception:
            warnings.append("Election queries failed; election fields incomplete.")
            confidence -= 0.10

    # Party control objects (executive + each legislature body)
    party_control: List[Dict[str, Any]] = []

    exec_party = gov.get("executiveControllerParty") or "unknown"
    party_control.append({
        "body": "Executive",
        "controller": exec_party,
        "controlType": "leader-party",
        "notes": "Derived from Head of Government party (or Head of State if HoG missing).",
    })

    legs = gov.get("legislatureBodies") or []
    if not legs:
        legs = ["Legislature"]

    for leg in legs:
        party_control.append({
            "body": leg,
            "controller": leg_control.get("winner") or "unknown",
            "controlType": "last-election-winner",
            "notes": leg_control.get("notes") or "Approximate from last legislative election winner.",
            "sourceElection": {
                "name": leg_control.get("electionName"),
                "date": leg_control.get("electionDate"),
            }
        })

    # Freedom rating
    freedom = freedom_house_rating(country_name, pr_table, cl_table)
    if freedom.get("score") is None:
        warnings.append("Political freedom rating unavailable (OWID Freedom House tables missing this entity).")
        confidence -= 0.05

    # News (5 unique topics)
    try:
        stories = gdelt_top_unique_topics(country_name, n=TOP_NEWS_N)
        if len(stories) < TOP_NEWS_N:
            warnings.append(f"Fewer than {TOP_NEWS_N} recent unique English stories found via GDELT.")
            confidence -= 0.05
    except Exception:
        stories = []
        warnings.append("GDELT fetch failed; news empty.")
        confidence -= 0.15

    confidence = max(0.0, min(1.0, confidence))

    return {
        "country": country_name,
        "iso2": iso2,
        "description": desc,
        "government": {
            "politicalSystem": gov.get("politicalSystem", "unknown"),
            "leaders": gov.get("leaders", []),
            "leadersSamePerson": gov.get("leadersSamePerson", False),
            "leaderNotes": gov.get("leaderNotes", ""),
            "partyControl": party_control,
        },
        "elections": {
            "nextNationalElection": next_elec,
            "legislatureControlBasis": leg_control,
        },
        "politicalFreedomRating": freedom,
        "news": {
            "topStoriesPastNd": stories,
            "windowDays": NEWS_WINDOW_DAYS,
            "count": TOP_NEWS_N,
            "method": "gdelt_doc_api",
            "englishOnly": True,
            "dedupe": "topic_cluster_from_titles",
        },
        "quality": {
            "confidence": round(confidence, 2),
            "warnings": warnings,
            "lastSuccessfulUpdate": iso_z(now_utc()),
        },
    }

def main():
    out: Dict[str, Any] = {
        "generatedAt": iso_z(now_utc()),
        "countries": [],
        "sources": {
            "wikidata": WIKIDATA_SPARQL,
            "wikipedia_summary": "en.wikipedia.org REST summary",
            "freedom_house_via_owid": {
                "pr_csv": OWID_FH_PR_CSV,
                "cl_csv": OWID_FH_CL_CSV,
                "notes": "Computed PR+CL total (0–100).",
            },
            "gdelt_doc_api": GDELT_DOC,
        }
    }

    print("▶ Loading Freedom House (OWID) tables...")
    pr_table, cl_table = load_freedom_house_tables()
    if not pr_table or not cl_table:
        print("⚠ Could not load one or both Freedom House tables; ratings may be missing.")

    for c in COUNTRIES:
        name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building snapshot for {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2, pr_table, cl_table))
        time.sleep(0.25)  # small delay to reduce rate-limit issues

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()


