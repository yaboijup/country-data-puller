"""
Build a Base44-friendly JSON snapshot for multiple countries.

Output: public/countries_snapshot.json

Fields returned per country:
- Head of State (+ party)
- Head of Government (+ party)
- Legislative body/bodies
- Party/group in charge of legislature body/bodies (best-effort via Wikidata last legislative election winner)
- Executive party/leader (best-effort: HoG party -> fallback HoS party)
- Freedom House score (numeric + qualitative), from Freedom House country page for (current_year - 1)
- Political system type (Wikidata P122 labels)
- Next legislative election (date + election type + exists?)
- Next executive election (date + election type + exists?)

Data sources:
- Wikidata SPARQL (government structure, leaders, parties, political system, elections)
- Freedom House website (Freedom in the World pages)
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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
FREEDOM_HOUSE_BASE = "https://freedomhouse.org/country"


# ---------------------------- COUNTRY LIST ----------------------------

COUNTRIES: List[Dict[str, str]] = [
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

# Freedom House slug exceptions (everything else can be slugified)
FH_SLUG_OVERRIDES: Dict[str, str] = {
    "UAE": "united-arab-emirates",
    "United Kingdom": "united-kingdom",
    "South Korea": "south-korea",
    "North Korea": "north-korea",
    "El Salvador": "el-salvador",
}

# Some country names don’t match Wikidata/other naming perfectly, but ISO2 lookup should work.
# This is only for Freedom House URL building.
def fh_slug(country_name: str) -> str:
    if country_name in FH_SLUG_OVERRIDES:
        return FH_SLUG_OVERRIDES[country_name]
    # basic slugify
    s = country_name.strip().lower()
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


# ---------------------------- HELPERS ----------------------------

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
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT, allow_redirects=True)
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
    - Head of State (P35) + party (P102)
    - Head of Government (P6) + party (P102)
    - Legislature bodies (P194)
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

    return {
        "headOfState": {"name": hos_name, "party": hos_party},
        "headOfGovernment": {"name": hog_name, "party": hog_party},
        "legislatureBodies": sorted(legislatures),
        "executiveController": {
            "leader": hog_name or hos_name,
            "partyOrGroup": hog_party or hos_party or "unknown",
            "method": "hog_party_else_hos_party",
        },
    }


# ---------------------------- ELECTIONS (Wikidata best-effort) ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def get_next_election_upcoming(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    kind = "executive" or "legislative"
    Uses Wikidata election items with:
    - jurisdiction (P1001)
    - point in time (P585)
    - instance of (P31) filters (best-effort)
    """
    today = _today_yyyymmdd()

    if kind == "executive":
        # presidential election, general election, election
        type_values = "wd:Q159821 wd:Q152203 wd:Q40231"
    else:
        # parliamentary election, legislative election, general election, election
        type_values = "wd:Q1079032 wd:Q104203 wd:Q152203 wd:Q40231"

    q = f"""
    SELECT ?eLabel ?date ?typeLabel WHERE {{
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
            "exists": "unknown",
            "nextDate": None,
            "electionType": None,
            "method": "wikidata_upcoming",
            "notes": "No upcoming election item found in Wikidata (common gap).",
        }

    b = bindings[0]
    return {
        "exists": True,
        "nextDate": _wd_val(b, "date"),
        "electionType": _wd_val(b, "typeLabel"),
        "method": "wikidata_upcoming",
        "notes": "From Wikidata upcoming election items (jurisdiction + future date).",
    }

def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    Approximate 'who controls parliament' using winner (P1346) of the most recent
    national legislative/parliamentary/general election.
    This can be wrong for coalitions / seat majorities.
    """
    today = _today_yyyymmdd()
    q = f"""
    SELECT ?eLabel ?date ?winnerLabel WHERE {{
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
            "method": "wikidata_last_leg_election_winner",
            "notes": "No prior legislative election item found in Wikidata.",
        }

    b = bindings[0]
    return {
        "winner": _wd_val(b, "winnerLabel") or "unknown",
        "method": "wikidata_last_leg_election_winner",
        "notes": "Approximate: last national legislative election winner (Wikidata P1346). Coalitions/seat majorities may differ.",
        "basis": {
            "electionName": _wd_val(b, "eLabel"),
            "electionDate": _wd_val(b, "date"),
        },
    }


# ---------------------------- FREEDOM HOUSE (site, year = current-1) ----------------------------

STATUS_RE = r"(Free|Partly Free|Not Free)"

def freedom_house_year() -> int:
    return date.today().year - 1

def freedom_house_url(country_name: str) -> str:
    y = freedom_house_year()
    slug = fh_slug(country_name)
    return f"{FREEDOM_HOUSE_BASE}/{slug}/freedom-world/{y}"

def parse_fh_score_and_status(html: str) -> (Optional[int], Optional[str]):
    """
    Best-effort parsing from Freedom House country page.
    Looks for common patterns near the top and/or "Global Freedom Score" block.
    """
    text = re.sub(r"[ \t]+", " ", html)
    text = re.sub(r"\r", "", text)

    # Pattern A: "Partly Free 54 100"
    m = re.search(rf"{STATUS_RE}\s+(\d{{1,3}})\s+100", text, flags=re.IGNORECASE)
    if m:
        status_raw = m.group(1).lower()
        score = int(m.group(2))
        status = {"free": "Free", "partly free": "Partly Free", "not free": "Not Free"}[status_raw]
        return score, status

    # Pattern B: "Global Freedom Score 54 100 Partly Free"
    m = re.search(rf"Global Freedom Score\s+(\d{{1,3}})\s+100\s+{STATUS_RE}", text, flags=re.IGNORECASE)
    if m:
        score = int(m.group(1))
        status_raw = m.group(2).lower()
        status = {"free": "Free", "partly free": "Partly Free", "not free": "Not Free"}[status_raw]
        return score, status

    return None, None

def fetch_freedom_house(country_name: str) -> Dict[str, Any]:
    url = freedom_house_url(country_name)
    html = req_text(url)
    if not html:
        return {
            "score": None,
            "status": "unknown",
            "year": freedom_house_year(),
            "source": url,
            "notes": "Failed to fetch Freedom House page.",
        }

    score, status = parse_fh_score_and_status(html)
    if score is None or status is None:
        return {
            "score": None,
            "status": "unknown",
            "year": freedom_house_year(),
            "source": url,
            "notes": "Fetched page but could not parse score/status (site layout may have changed).",
        }

    return {
        "score": score,
        "status": status,
        "year": freedom_house_year(),
        "source": url,
        "notes": None,
    }


# ---------------------------- BUILD ----------------------------

def build_country(country_name: str, iso2: str) -> Dict[str, Any]:
    qid = get_wikidata_country_qid_by_iso2(iso2)

    political_systems: List[str] = []
    gov: Dict[str, Any] = {
        "headOfState": {"name": None, "party": None},
        "headOfGovernment": {"name": None, "party": None},
        "legislatureBodies": [],
        "executiveController": {"leader": None, "partyOrGroup": "unknown", "method": "hog_party_else_hos_party"},
    }

    elections_exec = {"exists": "unknown", "nextDate": None, "electionType": None, "method": "wikidata_upcoming", "notes": "unknown"}
    elections_leg = {"exists": "unknown", "nextDate": None, "electionType": None, "method": "wikidata_upcoming", "notes": "unknown"}

    leg_control = {"winner": "unknown", "method": "wikidata_last_leg_election_winner", "notes": "unknown"}

    if qid:
        political_systems = get_political_system_labels(qid) or ["unknown"]
        gov = get_government_snapshot(qid)

        elections_exec = get_next_election_upcoming(qid, "executive")
        elections_leg = get_next_election_upcoming(qid, "legislative")
        leg_control = get_last_legislative_election_winner(qid)
    else:
        political_systems = ["unknown"]

    # Map legislature bodies -> controller
    bodies = gov.get("legislatureBodies") or []
    if not bodies:
        bodies = ["Legislature"]

    legislature = []
    for b in bodies:
        legislature.append({
            "name": b,
            "inControl": leg_control.get("winner", "unknown"),
            "controlMethod": leg_control.get("method"),
            "controlNotes": leg_control.get("notes"),
            "controlBasis": leg_control.get("basis"),
        })

    fh = fetch_freedom_house(country_name)

    return {
        "country": country_name,
        "iso2": iso2,
        "politicalSystem": {
            "values": political_systems or ["unknown"],
            "source": "wikidata:P122",
        },
        "executive": {
            "headOfState": {
                "name": gov["headOfState"].get("name"),
                "partyOrGroup": gov["headOfState"].get("party") or "unknown",
                "source": "wikidata:P35 (+party P102)",
            },
            "headOfGovernment": {
                "name": gov["headOfGovernment"].get("name"),
                "partyOrGroup": gov["headOfGovernment"].get("party") or "unknown",
                "source": "wikidata:P6 (+party P102)",
            },
            "executiveInPower": {
                "leader": gov["executiveController"].get("leader"),
                "partyOrGroup": gov["executiveController"].get("partyOrGroup") or "unknown",
                "method": gov["executiveController"].get("method"),
            },
        },
        "legislature": {
            "bodies": legislature,
            "source": "wikidata:P194 (+control best-effort via elections winner P1346)",
        },
        "freedomHouse": {
            "score": fh["score"],
            "status": fh["status"],
            "year": fh["year"],
            "source": fh["source"],
            "notes": fh["notes"],
        },
        "elections": {
            "legislative": {
                "exists": elections_leg["exists"],
                "nextDate": elections_leg["nextDate"],
                "electionType": elections_leg["electionType"],
                "method": elections_leg["method"],
                "notes": elections_leg["notes"],
                "source": "wikidata:P1001,P585,P31",
            },
            "executive": {
                "exists": elections_exec["exists"],
                "nextDate": elections_exec["nextDate"],
                "electionType": elections_exec["electionType"],
                "method": elections_exec["method"],
                "notes": elections_exec["notes"],
                "source": "wikidata:P1001,P585,P31",
            },
        },
    }

def main() -> None:
    out = {
        "generatedAt": iso_z(now_utc()),
        "freedomHouseYearRule": "current_year_minus_1",
        "countries": [],
        "sources": {
            "wikidata_sparql": WIKIDATA_SPARQL,
            "freedom_house_base": FREEDOM_HOUSE_BASE,
        },
    }

    for c in COUNTRIES:
        name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2))
        time.sleep(0.2)  # gentle rate limiting

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
