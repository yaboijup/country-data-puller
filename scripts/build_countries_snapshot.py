"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

Key changes vs your original:
- No raw nulls for the hard fields (party control + elections). Instead returns structured objects:
  { status: "ok|computed|unknown|not_applicable", value/date: ..., reason: ..., sources: [...] }
- More robust Wikidata election queries:
  - "next national election": finds soonest upcoming election item with country as jurisdiction (P1001)
    and a date (P585/P580/P571), using instance-of/subclass-of election.
- If no upcoming election is found:
  - checks whether any past national election exists
  - if none + system strongly suggests non-electoral governance -> not_applicable ("does not hold national general elections")
  - otherwise -> unknown (Wikidata gap)
- Executive controlling party: computed from Head of Government party (P6 -> P102),
  falling back to Head of State party (P35 -> P102) if HoG party missing.
- Legislature control still uses "winner of most recent legislative election" (P1346) proxy,
  but returns structured object rather than "unknown"/null.
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

WINDOW_HOURS = 24
NEWS_WINDOW_DAYS = 3
TOP_TRENDS_N = 10
TOP_NEWS_N = 3

ENGLISH_NEWS_ONLY = True

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY_BASE = "https://www.wikidata.org/wiki/"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

WIKI_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"

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

def pick_primary_query(country_name: str) -> str:
    aliases = QUERY_ALIASES.get(country_name)
    return aliases[0] if aliases else country_name

def field_value(
    value: Any = None,
    *,
    status: str = "ok",
    reason: Optional[str] = None,
    sources: Optional[List[str]] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """
    A stable object wrapper so you never emit raw nulls for important fields.
    """
    out: Dict[str, Any] = {"status": status}
    if sources is None:
        sources = []
    out["sources"] = sources

    # Put the primary payload under "value" by default, unless caller uses date/name/type fields separately.
    if "date" in extra or "name" in extra or "type" in extra:
        # allow richer objects that don't use "value"
        pass
    else:
        out["value"] = value if value is not None else ""

    if reason:
        out["reason"] = reason

    out.update(extra)
    return out


# ---------------------------- LANGUAGE / ENGLISH FILTER ----------------------------

def language_guess(text: str) -> str:
    if not text:
        return "unknown"
    t = text.strip()
    if re.search(r"[А-Яа-я]", t):
        return "ru"
    if re.search(r"[\u0600-\u06FF]", t):
        return "ar"
    if re.search(r"[\u4e00-\u9fff]", t):
        return "zh"
    if re.search(r"[\u3040-\u30ff]", t):
        return "ja"
    if re.search(r"[\uac00-\ud7af]", t):
        return "ko"
    if re.search(r"[A-Za-z]", t):
        return "en"
    return "unknown"

def needs_translation_to_english(text: str) -> bool:
    return language_guess(text) not in ("en", "unknown")

def is_english_script(text: str) -> bool:
    if not text:
        return False
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

def get_political_system_single(country_qid: str) -> Dict[str, Any]:
    """
    Return ONE political system label (English) using wdt:P122 (basic form of government).
    We avoid statement-rank complexity here for robustness.
    """
    q = f"""
    SELECT ?polsys ?polsysLabel WHERE {{
      wd:{country_qid} wdt:P122 ?polsys .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return field_value(
            "",
            status="unknown",
            reason="no_basic_form_of_government_in_wikidata",
            sources=[WIKIDATA_ENTITY_BASE + country_qid],
        )
    polsys_uri = _wd_val(bindings[0], "polsys")
    polsys_label = _wd_val(bindings[0], "polsysLabel") or ""
    sources = [WIKIDATA_ENTITY_BASE + country_qid]
    if polsys_uri and polsys_uri.startswith("http"):
        sources.append(polsys_uri.replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE))
    return field_value(polsys_label, status="ok", sources=sources)

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    """
    English labels:
    - head of state (P35) + party (P102)
    - head of government (P6) + party (P102)
    - legislature bodies (P194) list (names)
    """
    q = f"""
    SELECT
      ?hos ?hosLabel ?hosParty ?hosPartyLabel
      ?hog ?hogLabel ?hogParty ?hogPartyLabel
      ?leg ?legLabel
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
    LIMIT 300
    """

    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    hos_name = None
    hos_party = None
    hos_uri = None
    hos_party_uri = None

    hog_name = None
    hog_party = None
    hog_uri = None
    hog_party_uri = None

    legislatures = set()

    for b in bindings:
        if not hos_name:
            hos_name = _wd_val(b, "hosLabel")
            hos_uri = _wd_val(b, "hos")
        if not hos_party:
            hos_party = _wd_val(b, "hosPartyLabel")
            hos_party_uri = _wd_val(b, "hosParty")

        if not hog_name:
            hog_name = _wd_val(b, "hogLabel")
            hog_uri = _wd_val(b, "hog")
        if not hog_party:
            hog_party = _wd_val(b, "hogPartyLabel")
            hog_party_uri = _wd_val(b, "hogParty")

        leg = _wd_val(b, "legLabel")
        if leg:
            legislatures.add(leg)

    same_person = bool(hos_name and hog_name and hos_name == hog_name)

    leaders: List[Dict[str, Any]] = []
    if hos_name:
        leaders.append({
            "name": hos_name,
            "title": "Head of State",
            "party": hos_party or "",
            "sources": [x for x in [
                WIKIDATA_ENTITY_BASE + country_qid,
                (hos_uri or "").replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE) if hos_uri else "",
                (hos_party_uri or "").replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE) if hos_party_uri else "",
            ] if x],
        })
    if hog_name:
        leaders.append({
            "name": hog_name,
            "title": "Head of Government",
            "party": hog_party or "",
            "sources": [x for x in [
                WIKIDATA_ENTITY_BASE + country_qid,
                (hog_uri or "").replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE) if hog_uri else "",
                (hog_party_uri or "").replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE) if hog_party_uri else "",
            ] if x],
        })

    leader_notes = ""
    if same_person:
        leader_notes = "Head of State and Head of Government are the same person."

    # Executive controller party as a structured field
    exec_sources = [WIKIDATA_ENTITY_BASE + country_qid]
    if hog_party:
        status = "ok"
        reason = None
        value = hog_party
    elif hos_party:
        status = "computed"
        reason = "head_of_government_party_missing_used_head_of_state_party"
        value = hos_party
    else:
        status = "unknown"
        reason = "no_party_membership_found_for_leaders"
        value = ""

    executive_controller = field_value(value, status=status, reason=reason, sources=exec_sources)

    return {
        "leaders": leaders,
        "leadersSamePerson": same_person,
        "leaderNotes": leader_notes,
        "executiveControllerParty": executive_controller,
        "legislatureBodies": sorted(legislatures),
    }


# ---------------------------- ELECTIONS (WIKIDATA, BEST-EFFORT) ----------------------------

def _today_iso_floor() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def _date_key_iso(s: str) -> str:
    # leave as-is; Wikidata returns xsd:dateTime strings usually
    return s

def infer_no_national_elections_from_system(political_system_label: str) -> Optional[bool]:
    """
    Heuristic only. Returns:
      False => strongly suggests no meaningful national elections
      None  => unsure
    """
    s = (political_system_label or "").lower()
    hard_no = [
        "absolute monarchy",
        "military dictatorship",
        "junta",
        "one-party state",
        "one party state",
        "totalitarian",
    ]
    if any(t in s for t in hard_no):
        return False
    return None

def _any_past_national_election_exists(country_qid: str) -> bool:
    """
    Checks if there exists at least one election item in the past for this country jurisdiction.
    """
    today = _today_iso_floor()
    q = f"""
    SELECT ?e WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P31/wdt:P279* wd:Q40231 .
      ?e wdt:P585 ?date .
      FILTER(?date < "{today}"^^xsd:dateTime)
    }}
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    return bool(bindings)

def get_next_national_election(country_qid: str, political_system_label: str) -> Dict[str, Any]:
    """
    Find the soonest upcoming election item for this jurisdiction (P1001) with a date.

    We accept date fields:
      - point in time (P585)
      - start time (P580)
      - inception (P571) (rare but better than nothing)

    If none found:
      - if no past national elections exist AND political system suggests non-electoral -> not_applicable
      - else unknown (Wikidata gap)
    """
    today = _today_iso_floor()

    q = f"""
    SELECT ?e ?eLabel ?date ?datePropLabel ?typeLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P31/wdt:P279* wd:Q40231 .

      OPTIONAL {{ ?e wdt:P585 ?d1 . }}
      OPTIONAL {{ ?e wdt:P580 ?d2 . }}
      OPTIONAL {{ ?e wdt:P571 ?d3 . }}

      BIND(COALESCE(?d1, ?d2, ?d3) AS ?date)
      FILTER(BOUND(?date))
      FILTER(?date >= "{today}"^^xsd:dateTime)

      OPTIONAL {{ ?e wdt:P31 ?type . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      BIND(IF(BOUND(?d1), "P585", IF(BOUND(?d2), "P580", "P571")) AS ?dateProp)
      BIND(?dateProp AS ?datePropLabel)
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    sources = [WIKIDATA_ENTITY_BASE + country_qid]

    if bindings:
        b = bindings[0]
        date = _wd_val(b, "date") or ""
        name = _wd_val(b, "eLabel") or ""
        typ = _wd_val(b, "typeLabel") or ""
        date_prop = _wd_val(b, "datePropLabel") or ""
        e_uri = _wd_val(b, "e") or ""
        if e_uri:
            sources.append(e_uri.replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE))

        return field_value(
            "",
            status="ok",
            sources=sources,
            date=_date_key_iso(date),
            name=name,
            type=typ,
            dateField=date_prop,
            notes="Best-effort from Wikidata upcoming election items for this country (jurisdiction P1001).",
        )

    # No upcoming election item found; decide how to classify
    has_past = _any_past_national_election_exists(country_qid)
    inferred = infer_no_national_elections_from_system(political_system_label)

    if (not has_past) and (inferred is False):
        return field_value(
            "",
            status="not_applicable",
            reason="political_system_suggests_no_national_general_elections",
            sources=sources,
            date="",
            name="",
            type="",
            dateField="",
            notes="No upcoming or past national election items found and political system suggests non-electoral governance.",
            holdsNationalGeneralElections=False,
            internalElections=field_value(
                "",
                status="unknown",
                reason="internal_elections_not_consistently_modeled_in_wikidata",
                sources=sources,
            ),
        )

    # Otherwise: unknown (data gap)
    return field_value(
        "",
        status="unknown",
        reason="no_upcoming_national_election_item_found_in_wikidata",
        sources=sources,
        date="",
        name="",
        type="",
        dateField="",
        notes="Some countries do not model future elections consistently in Wikidata. Consider adding a country-specific computed fallback.",
        holdsNationalGeneralElections=True if has_past else "unknown",
        internalElections=field_value(
            "",
            status="unknown",
            reason="internal_elections_not_consistently_modeled_in_wikidata",
            sources=sources,
        ),
    )

def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    Approximate legislature control using the winner (P1346) of the most recent legislative/parliamentary/general election.

    Returns structured object:
      { status, value, electionName, electionDate, reason, sources }
    """
    today = _today_iso_floor()
    q = f"""
    SELECT ?e ?eLabel ?date ?winner ?winnerLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P31/wdt:P279* wd:Q40231 .

      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)

      OPTIONAL {{ ?e wdt:P1346 ?winner . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    sources = [WIKIDATA_ENTITY_BASE + country_qid]

    if not bindings:
        return field_value(
            "",
            status="unknown",
            reason="no_prior_election_item_found_in_wikidata",
            sources=sources,
            electionName="",
            electionDate="",
            notes="No prior election item found (Wikidata gap).",
        )

    b = bindings[0]
    winner = _wd_val(b, "winnerLabel") or ""
    e_name = _wd_val(b, "eLabel") or ""
    e_date = _wd_val(b, "date") or ""
    e_uri = _wd_val(b, "e") or ""
    if e_uri:
        sources.append(e_uri.replace("http://www.wikidata.org/entity/", WIKIDATA_ENTITY_BASE))

    if winner:
        return field_value(
            winner,
            status="ok",
            sources=sources,
            electionName=e_name,
            electionDate=e_date,
            notes="Legislature control approximated from last national election winner (Wikidata P1346).",
        )

    return field_value(
        "",
        status="unknown",
        reason="election_winner_missing_in_wikidata",
        sources=sources,
        electionName=e_name,
        electionDate=e_date,
        notes="Found last election item but winner (P1346) is missing.",
    )


# ---------------------------- WIKIPEDIA DESCRIPTION ----------------------------

def wikipedia_summary(country_name: str) -> Dict[str, Any]:
    title = country_name.replace(" ", "_")
    data = req_json(WIKI_SUMMARY_API + title)
    if not isinstance(data, dict):
        return {"summary": "", "source": "wikipedia", "url": ""}

    extract = (data.get("extract") or "").strip()
    url = safe_get(data, "content_urls", "desktop", "page", default="") or ""

    if not extract and country_name in QUERY_ALIASES:
        alt = QUERY_ALIASES[country_name][0].replace(" ", "_")
        data2 = req_json(WIKI_SUMMARY_API + alt)
        if isinstance(data2, dict):
            extract = (data2.get("extract") or "").strip()
            url = safe_get(data2, "content_urls", "desktop", "page", default=url) or url

    return {"summary": extract or "", "source": "wikipedia", "url": url}


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
        return {
            "score": "",
            "status": "unknown",
            "year": "",
            "notes": "Freedom House score not found in OWID tables.",
        }

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
        "components": {"politicalRights": round(float(best_pr), 1), "civilLiberties": round(float(best_cl), 1)},
        "sourceEntityName": best_entity,
        "notes": "Computed as PR(0–40)+CL(0–60) using OWID grapher (Freedom House).",
    }


# ---------------------------- NEWS (GDELT) ----------------------------

def _country_queries(country_name: str) -> List[str]:
    return QUERY_ALIASES.get(country_name, [country_name])

def gdelt_recent_articles(country_name: str, max_pull: int = 80) -> List[Dict[str, Any]]:
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
        dt = (a.get("seendate") or a.get("date") or "").strip()
        source = (a.get("sourceCommonName") or a.get("source") or a.get("sourceCountry") or "").strip()

        if not title or not url:
            continue

        if ENGLISH_NEWS_ONLY and not is_english_script(title):
            continue

        key = (title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)

        lg = language_guess(title)
        out.append({
            "title": title,
            "url": url,
            "source": source[:80],
            "publishedAt": dt,
            "languageGuess": lg,
            "needsTranslation": needs_translation_to_english(title),
        })

    return out

def gdelt_top_stories(country_name: str, n: int = TOP_NEWS_N) -> List[Dict[str, Any]]:
    arts = gdelt_recent_articles(country_name, max_pull=120)
    return arts[:n]


# ---------------------------- TRENDS (PYTRENDS + PROXY) ----------------------------

def _init_pytrends():
    try:
        from pytrends.request import TrendReq  # type: ignore
        p = None
        proxy_env = os.getenv("PYTRENDS_PROXY", "").strip()
        if proxy_env:
            p = [proxy_env]
        return TrendReq(hl="en-US", tz=0, proxies=p, timeout=(10, 25), retries=0, backoff_factor=0)
    except Exception:
        return None

def trends_top_searches_google(country_iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    pytrends = _init_pytrends()
    if pytrends is None:
        return [], "pytrends_unavailable"

    attempts = [country_iso2.lower(), country_iso2.upper()]
    for pn in attempts:
        try:
            series = pytrends.today_searches(pn=pn)
            items: List[Dict[str, Any]] = []
            for i, q in enumerate(list(series)[:TOP_TRENDS_N], start=1):
                q = str(q).strip()
                lg = language_guess(q)
                items.append({
                    "query": q,
                    "rank": i,
                    "languageGuess": lg,
                    "needsTranslation": needs_translation_to_english(q),
                    "translationHint": "Translate to English in Base44" if needs_translation_to_english(q) else None,
                    "source": "google_trends",
                })
            if items:
                return items, "google_trends_today_searches"
        except Exception:
            continue

    try:
        daily = pytrends.daily_trends(country=country_iso2.upper())
        vals = []
        if hasattr(daily, "columns") and "trend" in daily.columns:
            vals = daily["trend"].tolist()
        elif hasattr(daily, "iloc"):
            vals = daily.iloc[:, 0].tolist()

        items = []
        for i, q in enumerate([str(x).strip() for x in vals[:TOP_TRENDS_N]], start=1):
            lg = language_guess(q)
            items.append({
                "query": q,
                "rank": i,
                "languageGuess": lg,
                "needsTranslation": needs_translation_to_english(q),
                "translationHint": "Translate to English in Base44" if needs_translation_to_english(q) else None,
                "source": "google_trends",
            })
        if items:
            return items, "google_trends_daily_trends"
    except Exception:
        pass

    return [], "google_trends_unavailable_for_geo"

def _extract_proxy_trends_from_titles(titles: List[str], n: int = TOP_TRENDS_N) -> List[Dict[str, Any]]:
    if ENGLISH_NEWS_ONLY:
        titles = [t for t in titles if is_english_script(t)]

    text = " ".join(titles)
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    tokens = [t.lower() for t in text.split() if len(t) >= 4]

    stop = {
        "after","with","from","that","this","they","their","have","will","would","could",
        "says","said","over","into","amid","more","than","about","what","when","been",
        "were","also","which","who","your","them","today","news","update","latest",
        "country","government","president","minister","prime","state",
    }

    counts: Dict[str, int] = {}
    for tok in tokens:
        if tok in stop:
            continue
        counts[tok] = counts.get(tok, 0) + 1

    top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]
    out = []
    for i, (tok, c) in enumerate(top, start=1):
        out.append({
            "query": tok,
            "rank": i,
            "count": c,
            "languageGuess": "en",
            "needsTranslation": False,
            "translationHint": None,
            "source": "news_proxy",
            "notes": "Proxy trend (derived from frequent terms in recent English headlines). Not actual search data.",
        })
    return out

def trends_top_searches(country_name: str, iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    items, method = trends_top_searches_google(iso2)
    if items:
        return items, method

    arts = gdelt_recent_articles(country_name, max_pull=120)
    titles = [a["title"] for a in arts[:35] if a.get("title")]
    proxy = _extract_proxy_trends_from_titles(titles, n=TOP_TRENDS_N)
    return proxy, "news_proxy_trends"

def us_interest(country_query: str) -> Tuple[Dict[str, Any], str]:
    pytrends = _init_pytrends()
    if pytrends is None:
        return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "pytrends_unavailable"

    try:
        kw = [country_query]
        pytrends.build_payload(kw_list=kw, timeframe="now 1-d", geo="US")
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "google_trends_interest_empty"
        series = df[country_query].tolist()
        latest = int(series[-1]) if series else 0
        spark = [int(x) for x in series[-24:]]
        return {"query": country_query, "window": "past_24h", "interestIndex": latest, "sparkline": spark}, "google_trends_interest_over_time_us"
    except Exception:
        return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "google_trends_interest_failed"


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

    # Wikidata
    qid = get_wikidata_country_qid_by_iso2(iso2)
    if not qid:
        warnings.append("Wikidata entity not found via ISO2; government/elections fields missing.")
        confidence -= 0.25

        political_system = field_value(
            "",
            status="unknown",
            reason="wikidata_country_qid_not_found",
            sources=[],
        )
        gov = {
            "politicalSystem": political_system,
            "leaders": [],
            "leadersSamePerson": False,
            "leaderNotes": "",
            "executiveControllerParty": field_value("", status="unknown", reason="wikidata_country_qid_not_found", sources=[]),
            "legislatureBodies": [],
        }
        next_elec = field_value(
            "",
            status="unknown",
            reason="wikidata_country_qid_not_found",
            sources=[],
            date="",
            name="",
            type="",
            dateField="",
            notes="Wikidata country not found, cannot query elections.",
        )
        leg_control = field_value(
            "",
            status="unknown",
            reason="wikidata_country_qid_not_found",
            sources=[],
            electionName="",
            electionDate="",
            notes="Wikidata country not found, cannot query legislative control.",
        )
    else:
        political_system = get_political_system_single(qid)
        gov = get_government_snapshot(qid)
        gov["politicalSystem"] = political_system

        if not gov.get("leaders"):
            warnings.append("Leader fields missing (Wikidata gaps).")
            confidence -= 0.10

        if political_system.get("status") != "ok":
            warnings.append("Political system missing from Wikidata.")
            confidence -= 0.05

        next_elec = get_next_national_election(qid, political_system.get("value", ""))
        leg_control = get_last_legislative_election_winner(qid)
        if leg_control.get("status") != "ok":
            warnings.append("Legislative control winner not found (Wikidata election winner missing).")
            confidence -= 0.05

    # Party control list (executive + each legislature body)
    party_control: List[Dict[str, Any]] = []

    party_control.append({
        "body": "Executive",
        "controller": gov.get("executiveControllerParty", field_value("", status="unknown")),
        "controlType": "leader-party",
        "notes": "Derived from Head of Government party (fallback: Head of State party).",
    })

    legs = gov.get("legislatureBodies") or []
    if not legs:
        legs = ["Legislature"]

    for leg in legs:
        party_control.append({
            "body": leg,
            "controller": leg_control,
            "controlType": "last-election-winner",
            "notes": "Approximate from last election winner (Wikidata P1346).",
        })

    # Freedom rating
    freedom = freedom_house_rating(country_name, pr_table, cl_table)
    if freedom.get("score") in (None, ""):
        warnings.append("Political freedom rating unavailable (OWID Freedom House tables missing this entity).")
        confidence -= 0.05

    # Trends
    trends_items, trends_method = trends_top_searches(country_name, iso2)
    if not trends_items:
        warnings.append("Top searches unavailable; proxy trends also empty.")
        confidence -= 0.10
    elif trends_method.startswith("news_proxy"):
        warnings.append("Top searches are proxy trends (news-derived), not actual search queries.")
        confidence -= 0.03

    # News
    try:
        stories = gdelt_top_stories(country_name)
        if len(stories) < TOP_NEWS_N:
            warnings.append("Fewer than 3 recent English stories found via GDELT.")
            confidence -= 0.05
    except Exception:
        stories = []
        warnings.append("GDELT fetch failed; news empty.")
        confidence -= 0.15

    # US interest
    primary_query = pick_primary_query(country_name)
    us_obj, us_method = us_interest(primary_query)
    if not us_obj.get("sparkline"):
        warnings.append("US search interest unavailable/empty via Google Trends (common in CI).")
        confidence -= 0.08

    confidence = max(0.0, min(1.0, confidence))

    return {
        "country": country_name,
        "iso2": iso2,
        "description": desc,
        "government": {
            "politicalSystem": gov.get("politicalSystem", field_value("", status="unknown")),
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
        "trends": {
            "topSearchesPast24h": trends_items,
            "method": trends_method,
        },
        "news": {
            "topStoriesPast3d": stories,
            "method": "gdelt_doc_api",
            "englishOnly": True,
        },
        "usInterest": {
            **us_obj,
            "method": us_method,
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
        "windowHours": WINDOW_HOURS,
        "countries": [],
        "sources": {
            "wikidata": WIKIDATA_SPARQL,
            "wikipedia_summary": "en.wikipedia.org REST summary",
            "freedom_house_via_owid": {
                "pr_csv": OWID_FH_PR_CSV,
                "cl_csv": OWID_FH_CL_CSV,
                "notes": "Computed PR+CL total (0–100).",
            }
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
        time.sleep(0.25)

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()

