"""
Build a Base44-friendly JSON snapshot for multiple countries.

Output: public/countries_snapshot.json

Fields returned per country:
- Head of State (+ party)  [Wikidata current statement, no end date]
- Head of Government (+ party) [Wikidata current statement, no end date]
- Legislature body/bodies (filtered to "legislature" items)
- Party/group in charge of legislature body/bodies (best-effort via last legislative/general election winner; often missing)
- Executive party/leader (best-effort: HoG party -> fallback HoS party)
- Freedom House score (numeric + qualitative)
  - Robust year handling: tries a small window of likely FIW years
  - Sticky behavior: if fetch/parse fails and prior exists, keep prior values
- Political system type (Wikidata P122 labels)
- Next legislative election (date + type + exists?)
- Next executive election (date + type + exists?)

Data sources:
- Wikidata SPARQL
- Freedom House website (Freedom in the World pages)
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------------------- CONFIG ----------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
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

def fh_slug(country_name: str) -> str:
    if country_name in FH_SLUG_OVERRIDES:
        return FH_SLUG_OVERRIDES[country_name]
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
    last_status = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT, allow_redirects=True)
            last_status = r.status_code
            if r.status_code == 200:
                return r.text
            # Treat common bot-block statuses as "no text"
            if r.status_code in (403, 429):
                return r.text  # still return HTML; looks_like_challenge() will catch often
        except requests.RequestException:
            pass
        _sleep_backoff(attempt)
    # nothing
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

def load_previous_snapshot(path: Path) -> Dict[str, Any]:
    """
    Load previous public/countries_snapshot.json (if present) so we can keep Freedom House
    scores when new runs are blocked / parse fails.
    """
    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        countries = data.get("countries", [])
        by_iso2: Dict[str, Any] = {}
        for c in countries:
            iso2 = c.get("iso2")
            if iso2:
                by_iso2[iso2] = c
        return by_iso2
    except Exception:
        return {}


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

def get_current_officeholder(country_qid: str, prop: str) -> Dict[str, Optional[str]]:
    """
    prop: 'P35' (head of state) or 'P6' (head of government)
    Pulls the statement with no end date (pq:P582), prefers latest start date (pq:P580).
    """
    q = f"""
    SELECT ?personLabel ?partyLabel ?start WHERE {{
      wd:{country_qid} p:{prop} ?stmt .
      ?stmt ps:{prop} ?person .
      FILTER NOT EXISTS {{ ?stmt pq:P582 ?end . }}
      OPTIONAL {{ ?stmt pq:P580 ?start . }}
      OPTIONAL {{ ?person wdt:P102 ?party . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?start)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"name": None, "party": None}
    b = bindings[0]
    return {
        "name": _wd_val(b, "personLabel"),
        "party": _wd_val(b, "partyLabel"),
    }

def get_legislature_bodies(country_qid: str) -> List[str]:
    """
    P194 is broad; filter to items that are (subclasses of) 'legislature' to avoid junk.
    Legislature item: wd:Q11204
    """
    q = f"""
    SELECT ?legLabel WHERE {{
      wd:{country_qid} wdt:P194 ?leg .
      ?leg wdt:P31/wdt:P279* wd:Q11204 .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    out: List[str] = []
    for b in bindings:
        lab = _wd_val(b, "legLabel")
        if lab and lab not in out:
            out.append(lab)
    return out

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    hos = get_current_officeholder(country_qid, "P35")
    hog = get_current_officeholder(country_qid, "P6")
    legislatures = get_legislature_bodies(country_qid)

    return {
        "headOfState": hos,
        "headOfGovernment": hog,
        "legislatureBodies": legislatures,
        "executiveController": {
            "leader": (hog.get("name") or hos.get("name")),
            "partyOrGroup": (hog.get("party") or hos.get("party") or "unknown"),
            "method": "hog_party_else_hos_party",
        },
    }


# ---------------------------- ELECTIONS (Wikidata best-effort) ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def get_next_election_upcoming(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    kind = "executive" or "legislative"

    Tightened types:
      - executive: presidential election (Q159821) + general election (Q152203)
      - legislative: parliamentary election (Q1079032) + legislative election (Q104203) + general election (Q152203)

    We deliberately avoid the generic "election" (Q40231) because it causes false positives.
    """
    today = _today_yyyymmdd()

    if kind == "executive":
        type_values = "wd:Q159821 wd:Q152203"
    else:
        type_values = "wd:Q1079032 wd:Q104203 wd:Q152203"

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
    Many election items do NOT have P1346, so unknown is common.
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


# ---------------------------- FREEDOM HOUSE (multi-year retry + parsing + sticky) ----------------------------

STATUS_RE = r"(Free|Partly Free|Not Free)"

def looks_like_challenge(html: str) -> bool:
    h = html.lower()
    return (
        "cf-browser-verification" in h
        or "cloudflare" in h
        or "attention required" in h
        or "verify you are human" in h
        or "captcha" in h
        or "access denied" in h
        or "blocked" in h
    )

def parse_fh_score_and_status(html: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Best-effort parsing from Freedom House country page.
    We try multiple common patterns so minor layout changes don't break us.
    """
    text = re.sub(r"[ \t]+", " ", html)
    text = re.sub(r"\r", "", text)

    # A) "Partly Free 54 100"
    m = re.search(rf"{STATUS_RE}\s+(\d{{1,3}})\s+100", text, flags=re.IGNORECASE)
    if m:
        status_raw = m.group(1).lower()
        score = int(m.group(2))
        status = {"free": "Free", "partly free": "Partly Free", "not free": "Not Free"}[status_raw]
        return score, status

    # B) "Global Freedom Score 54 100 Partly Free"
    m = re.search(rf"Global Freedom Score\s+(\d{{1,3}})\s+100\s+{STATUS_RE}", text, flags=re.IGNORECASE)
    if m:
        score = int(m.group(1))
        status_raw = m.group(2).lower()
        status = {"free": "Free", "partly free": "Partly Free", "not free": "Not Free"}[status_raw]
        return score, status

    # C) "Partly Free ... 54 / 100"
    m = re.search(rf"{STATUS_RE}.*?(\d{{1,3}})\s*/\s*100", text, flags=re.IGNORECASE)
    if m:
        status_raw = m.group(1).lower()
        score = int(m.group(2))
        status = {"free": "Free", "partly free": "Partly Free", "not free": "Not Free"}[status_raw]
        return score, status

    return None, None

def candidate_fh_years(preferred: Optional[int] = None) -> List[int]:
    """
    Try a small descending window of likely valid FIW years.
    Freedom House FIW is commonly (current year - 1) but can lag.
    We also sanity-clamp so we never "detect" something like 2014 as "latest".
    """
    this_year = date.today().year
    base = preferred or (this_year - 1)
    if base < this_year - 3 or base > this_year:
        base = this_year - 1
    return [base, base - 1, base - 2, base - 3]

def freedom_house_url_for_year(country_name: str, y: int) -> str:
    slug = fh_slug(country_name)
    return f"{FREEDOM_HOUSE_BASE}/{slug}/freedom-world/{y}"

def fetch_freedom_house(country_name: str, preferred_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Try multiple years until we successfully fetch + parse.
    Returns internal flags ok/blocked for sticky merging.
    """
    last: Dict[str, Any] = {
        "score": None,
        "status": "unknown",
        "year": candidate_fh_years(preferred_year)[0],
        "source": freedom_house_url_for_year(country_name, candidate_fh_years(preferred_year)[0]),
        "notes": "Failed to fetch Freedom House page.",
        "ok": False,
        "blocked": False,
    }

    for y in candidate_fh_years(preferred_year):
        url = freedom_house_url_for_year(country_name, y)
        html = req_text(url)

        if not html:
            last = {
                "score": None,
                "status": "unknown",
                "year": y,
                "source": url,
                "notes": "Failed to fetch Freedom House page.",
                "ok": False,
                "blocked": False,
            }
            continue

        if looks_like_challenge(html):
            last = {
                "score": None,
                "status": "unknown",
                "year": y,
                "source": url,
                "notes": "Blocked by anti-bot / challenge page (Cloudflare-like).",
                "ok": False,
                "blocked": True,
            }
            continue

        score, status = parse_fh_score_and_status(html)
        if score is None or status is None:
            last = {
                "score": None,
                "status": "unknown",
                "year": y,
                "source": url,
                "notes": "Fetched page but could not parse score/status (site layout may have changed).",
                "ok": False,
                "blocked": False,
            }
            continue

        return {
            "score": score,
            "status": status,
            "year": y,
            "source": url,
            "notes": None,
            "ok": True,
            "blocked": False,
        }

    return last

def merge_freedom_house_sticky(new_fh: Dict[str, Any], prev_country_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sticky rule:
      - If new fetch/parse is OK -> use it.
      - If new fetch/parse fails OR is blocked AND previous has a score -> keep the previous values.
      - Otherwise keep the new (null/unknown) so you can see it's missing.
    """
    prev_fh = (prev_country_obj or {}).get("freedomHouse") if isinstance(prev_country_obj, dict) else None
    prev_score = prev_fh.get("score") if isinstance(prev_fh, dict) else None

    if new_fh.get("ok") is True:
        return {k: new_fh.get(k) for k in ["score", "status", "year", "source", "notes"]}

    if prev_score is not None:
        kept = {
            "score": prev_fh.get("score"),
            "status": prev_fh.get("status"),
            "year": prev_fh.get("year"),
            "source": prev_fh.get("source"),
            "notes": f"Kept previous Freedom House rating because latest fetch/parse failed: {new_fh.get('notes')}",
        }
        return kept

    return {k: new_fh.get(k) for k in ["score", "status", "year", "source", "notes"]}


# ---------------------------- BUILD ----------------------------

def build_country(country_name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
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

    # Freedom House (sticky) with multi-year retry
    new_fh = fetch_freedom_house(country_name, preferred_year=None)
    prev_obj = prev_by_iso2.get(iso2)
    fh = merge_freedom_house_sticky(new_fh, prev_obj)

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
                "source": "wikidata:P35 (current statement; +party P102)",
            },
            "headOfGovernment": {
                "name": gov["headOfGovernment"].get("name"),
                "partyOrGroup": gov["headOfGovernment"].get("party") or "unknown",
                "source": "wikidata:P6 (current statement; +party P102)",
            },
            "executiveInPower": {
                "leader": gov["executiveController"].get("leader"),
                "partyOrGroup": gov["executiveController"].get("partyOrGroup") or "unknown",
                "method": gov["executiveController"].get("method"),
            },
        },
        "legislature": {
            "bodies": legislature,
            "source": "wikidata:P194 (filtered to legislature items) + control best-effort via elections winner P1346",
        },
        "freedomHouse": fh,
        "elections": {
            "legislative": {
                "exists": elections_leg["exists"],
                "nextDate": elections_leg["nextDate"],
                "electionType": elections_leg["electionType"],
                "method": elections_leg["method"],
                "notes": elections_leg["notes"],
                "source": "wikidata:P1001,P585,P31 (tightened types)",
            },
            "executive": {
                "exists": elections_exec["exists"],
                "nextDate": elections_exec["nextDate"],
                "electionType": elections_exec["electionType"],
                "method": elections_exec["method"],
                "notes": elections_exec["notes"],
                "source": "wikidata:P1001,P585,P31 (tightened types)",
            },
        },
    }

def main() -> None:
    out_path = Path("public") / "countries_snapshot.json"
    prev_by_iso2 = load_previous_snapshot(out_path)

    out = {
        "generatedAt": iso_z(now_utc()),
        "freedomHouseYearRule": "try_current_minus_1_then_backfill_3_years",
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
        out["countries"].append(build_country(name, iso2, prev_by_iso2))
        time.sleep(0.25)  # gentle rate limiting (helps Wikidata + FH)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
