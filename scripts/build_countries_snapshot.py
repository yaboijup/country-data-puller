"""
Build a Base44-friendly JSON snapshot for multiple countries.

Output: public/countries_snapshot.json

Fields returned per country:
- Head of State (+ party)  [Wikidata current statement, no end date]
- Head of Government (+ party) [Wikidata current statement, no end date]
- Legislature body/bodies (filtered to "legislature" items)
- Party/group in charge of legislature body/bodies (best-effort via last legislative/general election winner; often missing)
- Executive party/leader (best-effort: HoG party -> fallback HoS party)
- World Bank governance snapshot (WGI percentile ranks; overall + components)
  - Pulls latest non-null values
  - Sticky behavior: if fetch fails and prior exists, keep prior values
- Political system type (Wikidata P122 labels)
- Next legislative election (date + type + exists?)
- Next executive election (date + type + exists?)

Data sources:
- Wikidata SPARQL
- World Bank Indicators API (Worldwide Governance Indicators - WGI)
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
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
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE = "https://api.worldbank.org/v2"

# WGI percentile rank indicators (0..100)
WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability": "VA.PER.RNK",      # Voice and Accountability: Percentile Rank
    "politicalStability": "PV.PER.RNK",       # Political Stability and Absence of Violence/Terrorism: Percentile Rank
    "governmentEffectiveness": "GE.PER.RNK",  # Government Effectiveness: Percentile Rank
    "regulatoryQuality": "RQ.PER.RNK",        # Regulatory Quality: Percentile Rank
    "ruleOfLaw": "RL.PER.RNK",                # Rule of Law: Percentile Rank
    "controlOfCorruption": "CC.PER.RNK",      # Control of Corruption: Percentile Rank
}


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


# ---------------------------- HELPERS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[Any]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            # If WB returns 404/400 etc, don't hammer it
            if r.status_code in (400, 404):
                return None
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
    Load previous public/countries_snapshot.json (if present) so we can keep WB governance
    scores when new runs fail.
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


# ---------------------------- WORLD BANK (WGI governance) ----------------------------

def _wb_indicator_url(iso2: str, indicator: str) -> str:
    iso2_l = iso2.strip().lower()
    return f"{WORLD_BANK_BASE}/country/{iso2_l}/indicator/{indicator}"

def _parse_wb_series_latest(payload: Any) -> Tuple[Optional[float], Optional[int], Optional[str]]:
    """
    WB JSON responses look like:
      [ {metadata...}, [ {date: "2023", value: X, ...}, {date:"2022", value:...}, ... ] ]

    Returns (value, year_int, notes)
    """
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        return None, None, "Unexpected WB response shape."

    series = payload[1]
    for row in series:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        dt = row.get("date")
        if val is None or dt is None:
            continue
        try:
            year = int(dt)
        except Exception:
            year = None
        try:
            fval = float(val)
        except Exception:
            continue
        return fval, year, None

    return None, None, "No non-null value found."

def fetch_wb_indicator_latest(iso2: str, indicator: str) -> Dict[str, Any]:
    url = _wb_indicator_url(iso2, indicator)
    payload = req_json(url, params={"format": "json", "per_page": 60})
    if payload is None:
        return {"ok": False, "value": None, "year": None, "source": url, "notes": "Failed to fetch WB indicator."}

    value, year, notes = _parse_wb_series_latest(payload)
    if value is None or year is None:
        return {"ok": False, "value": None, "year": None, "source": url, "notes": notes or "Could not parse WB indicator."}

    return {"ok": True, "value": value, "year": year, "source": url, "notes": None}

def _band_from_percentile(p: float) -> str:
    # Simple, predictable buckets for UI
    if p >= 66.0:
        return "High"
    if p >= 33.0:
        return "Medium"
    return "Low"

def fetch_wb_wgi_percentiles(iso2: str) -> Dict[str, Any]:
    """
    Pulls latest non-null percentile ranks for all WGI dimensions.
    Produces:
      - components: {dimension: {indicator, value, year}}
      - overallPercentile: average of available components (0..100)
      - year: max year among available (usually same across)
    """
    components: Dict[str, Any] = {}
    years: List[int] = []
    values: List[float] = []
    sources: Dict[str, str] = {}

    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        res = fetch_wb_indicator_latest(iso2, code)
        sources[dim] = res.get("source")
        if res.get("ok") is True and res.get("value") is not None and res.get("year") is not None:
            v = float(res["value"])
            y = int(res["year"])
            components[dim] = {"indicator": code, "percentile": v, "year": y}
            years.append(y)
            values.append(v)
        else:
            components[dim] = {"indicator": code, "percentile": None, "year": None, "notes": res.get("notes")}

    if not values:
        return {
            "ok": False,
            "overallPercentile": None,
            "band": "unknown",
            "year": None,
            "components": components,
            "sources": sources,
            "notes": "No WGI percentile values available (WB may not have this entity / code).",
        }

    overall = sum(values) / len(values)
    yr = max(years) if years else None
    return {
        "ok": True,
        "overallPercentile": round(overall, 2),
        "band": _band_from_percentile(overall),
        "year": yr,
        "components": components,
        "sources": sources,
        "notes": None,
    }

def merge_wb_sticky(new_wb: Dict[str, Any], prev_country_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sticky rule:
      - If new fetch is OK -> use it.
      - If new fails AND previous has overallPercentile -> keep previous.
      - Otherwise keep new (so missingness is visible).
    """
    prev_wb = (prev_country_obj or {}).get("worldBankGovernance") if isinstance(prev_country_obj, dict) else None
    prev_overall = prev_wb.get("overallPercentile") if isinstance(prev_wb, dict) else None

    if new_wb.get("ok") is True:
        # Don't keep "ok" field in the final object (optional)
        out = dict(new_wb)
        out.pop("ok", None)
        return out

    if prev_overall is not None:
        kept = dict(prev_wb)
        kept["notes"] = f"Kept previous WB governance values because latest fetch failed: {new_wb.get('notes')}"
        return kept

    out = dict(new_wb)
    out.pop("ok", None)
    return out


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

    # World Bank WGI governance (sticky)
    new_wb = fetch_wb_wgi_percentiles(iso2)
    prev_obj = prev_by_iso2.get(iso2)
    wb_gov = merge_wb_sticky(new_wb, prev_obj)

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
        "worldBankGovernance": wb_gov,
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
        "worldBankYearRule": "latest_non_null_per_indicator",
        "countries": [],
        "sources": {
            "wikidata_sparql": WIKIDATA_SPARQL,
            "world_bank_base": WORLD_BANK_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
    }

    for c in COUNTRIES:
        name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2, prev_by_iso2))
        time.sleep(0.25)  # gentle rate limiting (helps Wikidata + WB)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
