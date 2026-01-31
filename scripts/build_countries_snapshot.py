"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json
Format:
{
  "generatedAt": "...Z",
  "windowHours": 24,
  "countries": [
    {
      "country": "Iran",
      "iso2": "IR",
      "government": {...},
      "trends": {...},
      "news": {...},
      "usInterest": {...},
      "quality": {...}
    }
  ]
}

Data sources (best-effort, resilient):
- Government/leaders/political system: Wikidata SPARQL
- News (last 3 days): GDELT 2.1 DOC API
- Trends + US interest: pytrends (Google Trends, unofficial; can be flaky in CI)

Notes:
- "Party control" is HARD to do universally without paid/parliament datasets.
  This script outputs:
    - Executive party (from leader party when available)
    - Legislature: "unknown" + a note (unless party control can be inferred)
- "Political skew" is heuristic; mapped when party ideologies are known in overrides.
  Otherwise "unknown/contested" and flagged with warnings.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
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

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

# Your country list from the UI screenshot.
# For stability + speed, we use ISO2 and (optionally) Wikidata QIDs.
# If you later add more countries, just append entries here.
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

# Query aliases can improve Trends/News accuracy for ambiguous names.
# (Add more as you notice issues.)
QUERY_ALIASES: Dict[str, List[str]] = {
    "UAE": ["United Arab Emirates", "UAE"],
    "United Kingdom": ["United Kingdom", "UK", "Britain"],
    "Palestine": ["Palestine", "Palestinian Territories"],
    "Taiwan": ["Taiwan", "Republic of China"],
    "North Korea": ["North Korea", "DPRK"],
    "South Korea": ["South Korea", "Republic of Korea"],
}

# Political skew mapping is inherently approximate.
# You can expand this over time as you see recurring parties.
PARTY_SKEW_OVERRIDES: Dict[str, str] = {
    # UK
    "Conservative Party (UK)": "center-right / right",
    "Labour Party (UK)": "center-left",
    "Liberal Democrats (UK)": "center / center-left",
    # US-ish examples (not in your list, but shows pattern)
    "Democratic Party (United States)": "center-left",
    "Republican Party (United States)": "center-right / right",
    # Germany
    "Christian Democratic Union of Germany": "center-right",
    "Social Democratic Party of Germany": "center-left",
    "Alliance 90/The Greens": "left / center-left",
    # Japan
    "Liberal Democratic Party (Japan)": "center-right",
}

# ---------------------------- HELPERS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

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
        time.sleep(RETRY_SLEEP * attempt)
    return None

def req_text(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[str]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200 and r.text:
                return r.text
        except requests.RequestException:
            pass
        time.sleep(RETRY_SLEEP * attempt)
    return None

def safe_get(d: dict, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def pick_primary_query(country_name: str) -> str:
    aliases = QUERY_ALIASES.get(country_name)
    if aliases:
        return aliases[0]
    return country_name

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

def get_wikidata_country_qid(country_label: str, iso2: str) -> Optional[str]:
    """
    Resolve the country entity by ISO2 first (more stable than label).
    """
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

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    """
    Pull:
    - political system (P122)
    - head of state (P35)
    - head of government (P6)
    - legislature (P194)
    - (best-effort) next election (varies; may be empty)
    - leader parties (P102)
    """
    # We also grab labels in English.
    q = f"""
    SELECT
      ?polsysLabel
      ?hosLabel ?hosTitleLabel ?hosPartyLabel
      ?hogLabel ?hogTitleLabel ?hogPartyLabel
      ?legLabel
    WHERE {{
      OPTIONAL {{
        wd:{country_qid} wdt:P122 ?polsys .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P35 ?hos .
        OPTIONAL {{ ?hos wdt:P39 ?hosTitle . }}
        OPTIONAL {{ ?hos wdt:P102 ?hosParty . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P6 ?hog .
        OPTIONAL {{ ?hog wdt:P39 ?hogTitle . }}
        OPTIONAL {{ ?hog wdt:P102 ?hogParty . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P194 ?leg .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}
    }}
    LIMIT 50
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    # Aggregate (because SPARQL can return multiple rows).
    pol_systems = set()
    legislatures = set()

    hos_name = None
    hos_party = None
    hog_name = None
    hog_party = None

    # Titles from P39 are noisy; we’ll keep them only if they look plausible.
    hos_title = None
    hog_title = None

    for b in bindings:
        ps = _wd_val(b, "polsysLabel")
        if ps:
            pol_systems.add(ps)

        leg = _wd_val(b, "legLabel")
        if leg:
            legislatures.add(leg)

        # heads may repeat; choose the first non-empty.
        if not hos_name:
            hos_name = _wd_val(b, "hosLabel")
        if not hos_title:
            hos_title = _wd_val(b, "hosTitleLabel")
        if not hos_party:
            hos_party = _wd_val(b, "hosPartyLabel")

        if not hog_name:
            hog_name = _wd_val(b, "hogLabel")
        if not hog_title:
            hog_title = _wd_val(b, "hogTitleLabel")
        if not hog_party:
            hog_party = _wd_val(b, "hogPartyLabel")

    political_system = ", ".join(sorted(pol_systems)) if pol_systems else "unknown"

    leaders = []
    if hos_name:
        leaders.append({
            "name": hos_name,
            "title": hos_title or "Head of State",
            "isHeadOfState": True,
            "isHeadOfGovernment": False,
            "party": hos_party,
        })
    if hog_name and hog_name != hos_name:
        leaders.append({
            "name": hog_name,
            "title": hog_title or "Head of Government",
            "isHeadOfState": False,
            "isHeadOfGovernment": True,
            "party": hog_party,
        })

    # Party control (best effort): infer executive controlling party from HoG/HoS party
    executive_party = hog_party or hos_party

    party_control = []
    if executive_party:
        party_control.append({
            "body": "Executive (approx.)",
            "controller": executive_party,
            "controlType": "leader-party",
            "notes": "Derived from leader party; not a seat-count dataset."
        })
    # Legislature is usually present, but "control" needs seat data; we keep body + unknown controller
    for leg in sorted(legislatures):
        party_control.append({
            "body": leg,
            "controller": "unknown",
            "controlType": "unknown",
            "notes": "Seat/coalition control not reliably available via Wikidata alone."
        })

    # Political skew summary (heuristic from leader party)
    skew = "unknown/contested"
    if executive_party and executive_party in PARTY_SKEW_OVERRIDES:
        skew = PARTY_SKEW_OVERRIDES[executive_party]

    # Next election: Wikidata is inconsistent. We'll attempt a tiny best-effort query:
    # Look for "election" items in the country that have a future date (P585 = point in time).
    # This often returns nothing; we treat as unknown.
    next_election = {"date": None, "type": None, "notes": "unknown"}
    try:
        q2 = f"""
        SELECT ?eLabel ?date WHERE {{
          ?e wdt:P17 wd:{country_qid} .
          ?e wdt:P31/wdt:P279* wd:Q40231 .  # election (class/subclass)
          ?e wdt:P585 ?date .
          FILTER(?date > NOW())
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} ORDER BY ?date LIMIT 1
        """
        d2 = wikidata_sparql(q2)
        b2 = safe_get(d2, "results", "bindings", default=[])
        if b2:
            date_raw = _wd_val(b2[0], "date")
            e_label = _wd_val(b2[0], "eLabel") or "Election"
            if date_raw:
                # Keep YYYY-MM-DD if possible
                next_election = {
                    "date": date_raw[:10],
                    "type": e_label,
                    "notes": "Best-effort from Wikidata; verify with official election bodies."
                }
    except Exception:
        pass

    # Leader notes if none found
    leader_notes = ""
    if not leaders:
        leader_notes = "No definitive national leader found via Wikidata fields (P35/P6). Country may have collective leadership or missing data."

    return {
        "politicalSystem": political_system,
        "leaders": leaders,
        "leaderNotes": leader_notes,
        "politicalSkewSummary": skew,
        "partyControl": party_control,
        "nextElection": next_election,
    }


# ---------------------------- GDELT NEWS ----------------------------

def gdelt_top_stories(country_name: str, max_records: int = TOP_NEWS_N) -> List[Dict[str, str]]:
    """
    Uses GDELT DOC API with a 3-day window.
    We keep results simple: title/url/source/publishedAt.
    """
    start = (now_utc() - timedelta(days=NEWS_WINDOW_DAYS)).strftime("%Y%m%d%H%M%S")
    end = now_utc().strftime("%Y%m%d%H%M%S")

    # Use quoted phrase query to reduce noise, but allow aliases if configured.
    queries = QUERY_ALIASES.get(country_name, [country_name])
    # Build OR query: ("Iran" OR "Islamic Republic of Iran") etc.
    q = " OR ".join([f'"{x}"' for x in queries])

    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records * 5),  # pull extra then dedupe
        "startdatetime": start,
        "enddatetime": end,
        "sourcelang": "English",  # keep consistent for now; can remove for local-language
        "formatting": "json",
        "sort": "HybridRel",  # relevance + recency
    }

    data = req_json(GDELT_DOC, params=params)
    arts = safe_get(data, "articles", default=[]) if isinstance(data, dict) else []

    out: List[Dict[str, str]] = []
    seen = set()
    for a in arts:
        title = a.get("title") or ""
        url = a.get("url") or ""
        source = a.get("sourceCountry") or a.get("sourceCollection") or a.get("sourceCommonName") or a.get("source") or ""
        dt = a.get("seendate") or a.get("date") or ""

        key = (title.strip().lower(), url.strip().lower())
        if not title or not url or key in seen:
            continue
        seen.add(key)

        out.append({
            "title": title.strip(),
            "url": url.strip(),
            "source": str(source).strip()[:80] if source else "GDELT",
            "publishedAt": dt.strip(),
        })
        if len(out) >= TOP_NEWS_N:
            break
    return out


# ---------------------------- TRENDS (PYTRENDS) ----------------------------

def _init_pytrends():
    """
    Lazy import so the whole job doesn't fail if pytrends is blocked.
    """
    try:
        from pytrends.request import TrendReq  # type: ignore
        proxy = os.getenv("PYTRENDS_PROXY", "").strip()
        proxies = [proxy] if proxy else None
        return TrendReq(hl="en-US", tz=0, proxies=proxies, timeout=(10, 25), retries=0, backoff_factor=0)
    except Exception:
        return None

def trends_top_searches(country_iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    """
    Best-effort top searches in the last ~day.
    pytrends daily_trends returns "today's" trends for certain geos.
    Many geos return empty or error; we return [] with a note.
    """
    pytrends = _init_pytrends()
    if pytrends is None:
        return [], "pytrends unavailable"

    try:
        df = pytrends.today_searches(pn=country_iso2.lower())
        # today_searches returns a list-like series depending on version.
        items = []
        for i, q in enumerate(list(df)[:TOP_TRENDS_N], start=1):
            items.append({"query": str(q), "rank": i})
        return items, "google_trends_today_searches"
    except Exception:
        # fallback: daily_trends exists for some countries (geo must be like 'UNITED_STATES' etc.)
        try:
            daily = pytrends.daily_trends(country=country_iso2.upper())
            # daily_trends returns a dataframe with a 'trend' column in some versions.
            if "trend" in daily.columns:
                vals = daily["trend"].tolist()
            else:
                vals = daily.iloc[:, 0].tolist()
            items = [{"query": str(q), "rank": i} for i, q in enumerate(vals[:TOP_TRENDS_N], start=1)]
            return items, "google_trends_daily_trends"
        except Exception:
            return [], "google_trends_unavailable_for_geo"

def us_interest(country_query: str) -> Tuple[Dict[str, Any], str]:
    """
    US interest (past 24 hours). Returns:
      - interestIndex (latest)
      - sparkline (list of ints)
    """
    pytrends = _init_pytrends()
    if pytrends is None:
        return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "pytrends unavailable"

    try:
        kw = [country_query]
        pytrends.build_payload(kw_list=kw, timeframe="now 1-d", geo="US")
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "google_trends_interest_empty"
        series = df[country_query].tolist()
        latest = int(series[-1]) if series else 0
        spark = [int(x) for x in series[-24:]]  # last 24 points if hourly-ish
        return {"query": country_query, "window": "past_24h", "interestIndex": latest, "sparkline": spark}, "google_trends_interest_over_time_us"
    except Exception:
        return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "google_trends_interest_failed"


# ---------------------------- BUILD ----------------------------

def build_country(country_name: str, iso2: str) -> Dict[str, Any]:
    warnings: List[str] = []
    confidence = 1.0

    # Government
    gov: Dict[str, Any] = {
        "politicalSystem": "unknown",
        "leaders": [],
        "leaderNotes": "",
        "politicalSkewSummary": "unknown/contested",
        "partyControl": [],
        "nextElection": {"date": None, "type": None, "notes": "unknown"},
    }

    qid = get_wikidata_country_qid(country_name, iso2)
    if not qid:
        warnings.append("Wikidata country entity not found via ISO2; government fields missing.")
        confidence -= 0.25
    else:
        try:
            gov = get_government_snapshot(qid)
            if gov.get("politicalSkewSummary") == "unknown/contested":
                warnings.append("Political skew is heuristic and may be contested or unavailable.")
                confidence -= 0.05
            if not gov.get("leaders"):
                warnings.append("Leader fields missing; country may have collective leadership or Wikidata gaps.")
                confidence -= 0.10
            # Next election often missing
            ne = gov.get("nextElection", {})
            if not ne or not ne.get("date"):
                warnings.append("Next election not reliably available; verify with official election bodies.")
                confidence -= 0.05
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.20

    # Trends (in-country)
    trends_items, trends_method = trends_top_searches(iso2)
    if not trends_items:
        warnings.append("Top searches unavailable for this geo via Google Trends (common in CI).")
        confidence -= 0.10

    # News (GDELT)
    try:
        stories = gdelt_top_stories(country_name)
        if len(stories) < TOP_NEWS_N:
            warnings.append("Fewer than 3 recent stories found via GDELT with current filters.")
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
        confidence -= 0.10

    confidence = max(0.0, min(1.0, confidence))

    return {
        "country": country_name,
        "iso2": iso2,
        "government": gov,
        "trends": {
            "topSearchesPast24h": trends_items,
            "method": trends_method,
        },
        "news": {
            "topStoriesPast3d": stories,
            "method": "gdelt_doc_api",
        },
        "usInterest": {
            **us_obj,
            "method": us_method,
        },
        "quality": {
            "confidence": round(confidence, 2),
            "warnings": warnings,
            "lastSuccessfulUpdate": iso_z(now_utc()),
        }
    }

def main():
    out = {
        "generatedAt": iso_z(now_utc()),
        "windowHours": WINDOW_HOURS,
        "countries": []
    }

    for c in COUNTRIES:
        country_name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building snapshot for {country_name} ({iso2}) ...")
        out["countries"].append(build_country(country_name, iso2))
        # tiny sleep to reduce rate-limit risk
        time.sleep(0.2)

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
