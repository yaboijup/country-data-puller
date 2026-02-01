"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

Focus (no news, no Wikipedia summary):
- Political system (Wikidata P122 labels)
- Head of State (P35) + party (P102)
- Head of Government (P6) + party (P102)
- Legislature bodies (P194 labels)
- Executive / Legislature "type" heuristics (best-effort, with nuance notes)
- Party control (best-effort):
  - Executive: HoG party (fallback HoS party)
  - Legislature: last national legislative election winner (P1346) from Wikidata (approximate!)
- Elections:
  - nextExecutiveElection (best-effort)
  - nextLegislativeElection (best-effort)
  - nuance flags when elections are non-competitive or not meaningful (heuristic)

- Freedom House rating:
  - PR (0–40) + CL (0–60) total (0–100) from OWID grapher CSVs
"""

from __future__ import annotations

import csv
import io
import json
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

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

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

def get_political_system_labels(country_qid: str) -> List[str]:
    """
    Return multiple political system labels (English) for nuance.
    """
    q = f"""
    SELECT ?polsysLabel WHERE {{
      wd:{country_qid} wdt:P122 ?polsys .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 20
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    out: List[str] = []
    for b in bindings:
        lab = _wd_val(b, "polsysLabel")
        if lab and lab not in out:
            out.append(lab)
    return out

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    """
    - head of state (P35) + party (P102)
    - head of government (P6) + party (P102)
    - legislature bodies (P194)
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

    leaders: List[Dict[str, Any]] = []
    if hos_name:
        leaders.append({"role": "head_of_state", "name": hos_name, "party": hos_party})
    if hog_name:
        leaders.append({"role": "head_of_government", "name": hog_name, "party": hog_party})

    return {
        "leaders": leaders,
        "leadersSamePerson": same_person,
        "executiveControllerParty": hog_party or hos_party or "unknown",
        "legislatureBodies": sorted(legislatures),
    }


# ---------------------------- STRUCTURE HEURISTICS (NUANCE) ----------------------------

def _has_label(labels: List[str], needle: str) -> bool:
    n = needle.lower()
    return any(n in (x or "").lower() for x in labels)

def classify_executive_type(political_systems: List[str], leaders: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Best-effort executive classification.
    Returns {type, notes}.
    """
    # Direct label hints
    if _has_label(political_systems, "absolute monarchy") or _has_label(political_systems, "monarchy"):
        return {"type": "monarchy", "notes": "Heuristic from political system labels (Wikidata P122)."}
    if _has_label(political_systems, "presidential system"):
        return {"type": "presidential", "notes": "Heuristic from political system labels (Wikidata P122)."}
    if _has_label(political_systems, "parliamentary system"):
        return {"type": "parliamentary", "notes": "Heuristic from political system labels (Wikidata P122)."}
    if _has_label(political_systems, "semi-presidential system"):
        return {"type": "semi-presidential", "notes": "Heuristic from political system labels (Wikidata P122)."}

    # Fallback: leader roles present
    has_hos = any(l.get("role") == "head_of_state" for l in leaders)
    has_hog = any(l.get("role") == "head_of_government" for l in leaders)

    if has_hos and has_hog:
        return {"type": "dual_executive", "notes": "Both head of state and head of government present; exact subtype may vary."}
    if has_hos and not has_hog:
        return {"type": "single_executive", "notes": "Only head of state present in Wikidata; could be monarchy or presidential republic (data gaps possible)."}
    if has_hog and not has_hos:
        return {"type": "single_executive", "notes": "Only head of government present in Wikidata; could reflect data gaps."}

    return {"type": "unknown", "notes": "Insufficient data to classify executive structure."}

def classify_legislature_type(leg_bodies: List[str]) -> Dict[str, Any]:
    """
    Very light inference: if 2+ bodies -> 'bicameral_or_multi', 1 -> 'unicameral_or_unknown'.
    """
    if not leg_bodies:
        return {"type": "unknown", "notes": "No legislature bodies listed in Wikidata (P194)."}
    if len(leg_bodies) >= 2:
        return {"type": "bicameral_or_multi", "notes": "Multiple legislative bodies listed (Wikidata P194)."}
    return {"type": "unicameral_or_unknown", "notes": "Single legislative body listed (Wikidata P194). Not always definitive."}


# ---------------------------- ELECTIONS ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def infer_elections_meaningful(political_systems: List[str]) -> Dict[str, Any]:
    """
    Returns: {meaningful: True/False/"unknown", notes}
    This does NOT claim "no elections occur"—it flags cases where national elections may be non-competitive.
    """
    s = " | ".join([x.lower() for x in political_systems])

    strong_noncompetitive = [
        "one-party state",
        "single-party state",
        "totalitarian",
        "military dictatorship",
        "military junta",
        "absolute monarchy",
    ]
    if any(t in s for t in strong_noncompetitive):
        return {
            "meaningful": False,
            "notes": "Political system labels suggest elections may be absent or non-competitive. Treat election dates (if any) cautiously.",
        }
    if political_systems:
        return {"meaningful": "unknown", "notes": "No strong non-competitive indicators in political system labels."}
    return {"meaningful": "unknown", "notes": "No political system labels available to infer competitiveness."}

def get_next_election_upcoming(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    kind = "executive" or "legislative"
    Uses Wikidata election items: jurisdiction (P1001) + date (P585) in the future.

    Executive: prefer presidential election / general election
    Legislative: prefer parliamentary/legislative/general election
    """
    today = _today_yyyymmdd()

    if kind == "executive":
        type_values = "wd:Q159821 wd:Q152203 wd:Q40231"  # presidential, general, election
        kind_note = "Executive election (best-effort via Wikidata types)."
    else:
        type_values = "wd:Q1079032 wd:Q104203 wd:Q152203 wd:Q40231"  # parliamentary, legislative, general, election
        kind_note = "Legislative election (best-effort via Wikidata types)."

    q = f"""
    SELECT ?e ?eLabel ?date ?typeLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date >= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{ {type_values} }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    if not bindings:
        return {
            "date": None,
            "name": None,
            "type": None,
            "method": "wikidata_upcoming",
            "notes": f"{kind_note} No upcoming election item found in Wikidata (common gap).",
        }

    b = bindings[0]
    return {
        "date": _wd_val(b, "date"),
        "name": _wd_val(b, "eLabel"),
        "type": _wd_val(b, "typeLabel"),
        "method": "wikidata_upcoming",
        "notes": f"{kind_note} From Wikidata upcoming election items (jurisdiction + future date).",
    }

def get_last_election_with_term_hint(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    Find most recent election of a kind and try to extract contested office (P541) and term length (P2097).
    """
    today = _today_yyyymmdd()

    if kind == "executive":
        type_values = "wd:Q159821 wd:Q152203"  # presidential, general
    else:
        type_values = "wd:Q1079032 wd:Q104203 wd:Q152203"  # parliamentary, legislative, general

    q = f"""
    SELECT ?e ?eLabel ?date ?typeLabel ?officeLabel ?termYears WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{ {type_values} }}

      OPTIONAL {{
        ?e wdt:P541 ?office .
        OPTIONAL {{ ?office wdt:P2097 ?termYears . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"found": False}

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

def estimate_next_election_from_last(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    Estimate: last election date + termYears (if available).
    """
    last = get_last_election_with_term_hint(country_qid, kind)
    if not last.get("found") or not last.get("electionDate"):
        return {
            "date": None,
            "name": None,
            "type": None,
            "method": "estimate_from_last_plus_term",
            "notes": "No prior election found to estimate from.",
            "basis": None,
        }

    term_years = last.get("termYears")
    if not term_years:
        return {
            "date": None,
            "name": None,
            "type": last.get("electionType"),
            "method": "estimate_from_last_plus_term",
            "notes": "Found last election, but term length of contested office missing (P2097). No estimate.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
            },
        }

    try:
        dt = datetime.fromisoformat(str(last["electionDate"]).replace("Z", "+00:00"))
        # integer year add if term looks integral; else day-based
        if abs(term_years - round(term_years)) < 1e-6:
            est = dt.replace(year=dt.year + int(round(term_years)))
        else:
            est = dt + timedelta(days=int(term_years * 365.25))

        return {
            "date": iso_z(est),
            "name": None,
            "type": last.get("electionType"),
            "method": "estimate_from_last_plus_term",
            "notes": "Estimated as last election date + term length of contested office (P541 + P2097). Treat as an estimate.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
                "termYears": term_years,
            },
        }
    except Exception:
        return {
            "date": None,
            "name": None,
            "type": last.get("electionType"),
            "method": "estimate_from_last_plus_term",
            "notes": "Failed computing estimate from last election + term length.",
            "basis": {
                "lastElectionName": last.get("electionName"),
                "lastElectionDate": last.get("electionDate"),
                "officeContested": last.get("officeContested"),
                "termYears": term_years,
            },
        }

def resolve_next_election(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    Resolver:
      1) upcoming item
      2) estimate from last+term (if possible)
      3) unknown
    """
    upcoming = get_next_election_upcoming(country_qid, kind)
    if upcoming.get("date"):
        return upcoming

    est = estimate_next_election_from_last(country_qid, kind)
    if est.get("date"):
        return est

    # preserve the more specific "no upcoming found" notes
    return upcoming


def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    Approximate legislature control using winner (P1346) of the most recent
    legislative/parliamentary/general election.
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
        return {
            "winner": "unknown",
            "electionName": None,
            "electionDate": None,
            "notes": "No prior legislative election item found in Wikidata.",
        }

    b = bindings[0]
    return {
        "winner": _wd_val(b, "winnerLabel") or "unknown",
        "electionName": _wd_val(b, "eLabel"),
        "electionDate": _wd_val(b, "date"),
        "notes": "Approximate: last national legislative election winner (Wikidata P1346). This can be wrong for coalitions/seat majorities.",
    }


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
            "score": None,
            "status": "unknown",
            "year": None,
            "components": None,
            "sourceEntityName": None,
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
        "components": {
            "politicalRights": round(float(best_pr), 1),
            "civilLiberties": round(float(best_cl), 1),
        },
        "sourceEntityName": best_entity,
        "notes": "Computed as PR(0–40)+CL(0–60) using OWID grapher (Freedom House).",
    }


# ---------------------------- BUILD ----------------------------

def build_country(country_name: str, iso2: str, pr_table: Dict[str, Dict[int, float]], cl_table: Dict[str, Dict[int, float]]) -> Dict[str, Any]:
    retrieved_at = iso_z(now_utc())
    warnings: List[str] = []
    confidence = 1.0

    qid = get_wikidata_country_qid_by_iso2(iso2)
    if not qid:
        warnings.append("Wikidata entity not found via ISO2; government/elections fields missing.")
        confidence -= 0.35

    political_systems: List[str] = []
    gov = {"leaders": [], "leadersSamePerson": False, "executiveControllerParty": "unknown", "legislatureBodies": []}

    if qid:
        try:
            political_systems = get_political_system_labels(qid)
            gov = get_government_snapshot(qid)
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.25

    if not political_systems:
        warnings.append("Political system labels missing (Wikidata P122 gaps).")
        confidence -= 0.05

    if not gov.get("leaders"):
        warnings.append("Leader fields missing (Wikidata P35/P6 gaps).")
        confidence -= 0.10

    # Structure heuristics
    exec_type = classify_executive_type(political_systems, gov.get("leaders") or [])
    leg_type = classify_legislature_type(gov.get("legislatureBodies") or [])

    # Elections
    elections_meaningful = infer_elections_meaningful(political_systems)

    next_exec = {"date": None, "name": None, "type": None, "method": None, "notes": "unknown"}
    next_leg = {"date": None, "name": None, "type": None, "method": None, "notes": "unknown"}
    leg_control_basis = {"winner": "unknown", "electionName": None, "electionDate": None, "notes": "unknown"}

    if qid:
        try:
            next_exec = resolve_next_election(qid, "executive")
            next_leg = resolve_next_election(qid, "legislative")
            leg_control_basis = get_last_legislative_election_winner(qid)

            if elections_meaningful.get("meaningful") is False:
                warnings.append("System likely non-competitive; election data (if present) may not reflect meaningful democratic turnover.")
                confidence -= 0.05

            if next_exec.get("date") is None:
                warnings.append("Next executive election date unavailable (Wikidata gaps and no term-length estimate).")
                confidence -= 0.04

            if next_leg.get("date") is None:
                warnings.append("Next legislative election date unavailable (Wikidata gaps and no term-length estimate).")
                confidence -= 0.04

            if leg_control_basis.get("winner") == "unknown":
                warnings.append("Legislature control not found (Wikidata winner P1346 missing).")
                confidence -= 0.04
        except Exception:
            warnings.append("Election queries failed; election fields incomplete.")
            confidence -= 0.12

    # Party control objects
    party_control: List[Dict[str, Any]] = []

    exec_party = gov.get("executiveControllerParty") or "unknown"
    party_control.append({
        "branch_or_body": "Executive",
        "controller": exec_party,
        "method": "leader_party",
        "notes": "Derived from Head of Government party (fallback Head of State party).",
    })

    leg_bodies = gov.get("legislatureBodies") or []
    if not leg_bodies:
        leg_bodies = ["Legislature"]

    for body in leg_bodies:
        party_control.append({
            "branch_or_body": body,
            "controller": leg_control_basis.get("winner") or "unknown",
            "method": "last_legislative_election_winner",
            "notes": leg_control_basis.get("notes") or "Approximate from last legislative election winner.",
            "basis": {
                "electionName": leg_control_basis.get("electionName"),
                "electionDate": leg_control_basis.get("electionDate"),
            },
        })

    # Freedom House
    freedom = freedom_house_rating(country_name, pr_table, cl_table)
    if freedom.get("score") is None:
        warnings.append("Freedom House score unavailable (OWID entity match missing).")
        confidence -= 0.05

    confidence = max(0.0, min(1.0, confidence))

    return {
        "country": country_name,
        "iso2": iso2,
        "retrievedAt": retrieved_at,
        "government": {
            "politicalSystem": {
                "values": political_systems or ["unknown"],
                "source": "wikidata:P122",
                "retrievedAt": retrieved_at,
            },
            "executive": {
                "leaders": gov.get("leaders", []),
                "leadersSamePerson": gov.get("leadersSamePerson", False),
                "type": exec_type,
                "source": "wikidata:P35,P6,P102 (+heuristics)",
                "retrievedAt": retrieved_at,
            },
            "legislature": {
                "bodies": gov.get("legislatureBodies", []),
                "type": leg_type,
                "source": "wikidata:P194 (+heuristics)",
                "retrievedAt": retrieved_at,
            },
            "partyControl": {
                "entries": party_control,
                "notes": "Legislature control is approximate; coalitions/seat majorities often not captured by winner fields.",
                "retrievedAt": retrieved_at,
            },
        },
        "elections": {
            "meaningful": elections_meaningful,
            "nextExecutiveElection": {
                **next_exec,
                "source": "wikidata:P1001,P585,P31 (+fallback estimate P541,P2097)",
                "retrievedAt": retrieved_at,
            },
            "nextLegislativeElection": {
                **next_leg,
                "source": "wikidata:P1001,P585,P31 (+fallback estimate P541,P2097)",
                "retrievedAt": retrieved_at,
            },
        },
        "freedomHouse": {
            **freedom,
            "source": "owid_grapher:political-rights-score-fh + civil-liberties-score-fh",
            "retrievedAt": retrieved_at,
        },
        "quality": {
            "confidence": round(confidence, 2),
            "warnings": warnings,
        },
    }

def main():
    generated_at = iso_z(now_utc())

    out: Dict[str, Any] = {
        "generatedAt": generated_at,
        "countries": [],
        "sources": {
            "wikidata_sparql": WIKIDATA_SPARQL,
            "freedom_house_via_owid": {
                "pr_csv": OWID_FH_PR_CSV,
                "cl_csv": OWID_FH_CL_CSV,
                "notes": "Computed PR+CL total (0–100).",
            },
        },
        "notes": [
            "This snapshot is best-effort and includes provenance per field.",
            "Legislature party control is approximate (often coalitions/seat majorities are not represented by a single winner field).",
            "Election dates are pulled from Wikidata where present; otherwise estimated from last election + term length when available.",
        ],
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
