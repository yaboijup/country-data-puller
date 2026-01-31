"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

What it does (best-effort):
- Government snapshot (leaders + political system + legislature) via Wikidata SPARQL
- 3 recent English news stories (last 3 days) via GDELT DOC 2.1
- "Top searches" (past ~24h) via Google Trends (pytrends) when available
  - If unavailable, fallback = "proxy trends" extracted from top recent news headlines
- US search interest (past 24h) via Google Trends interest_over_time when available

Notes:
- Real "party control of parliament" is not universally reliable from Wikidata alone.
  We output "executive party (approx.)" from leader party when available and mark legislature control as unknown.
- For countries where Google Trends is blocked/unavailable (common in CI), we degrade gracefully.
"""

from __future__ import annotations

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

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

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

PARTY_SKEW_OVERRIDES: Dict[str, str] = {
    "Conservative Party (UK)": "center-right / right",
    "Labour Party (UK)": "center-left",
    "Liberal Democrats (UK)": "center / center-left",
    "Christian Democratic Union of Germany": "center-right",
    "Social Democratic Party of Germany": "center-left",
    "Alliance 90/The Greens": "left / center-left",
    "Liberal Democratic Party (Japan)": "center-right",
}


# ---------------------------- BASICS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

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


# ---------------------------- ENGLISH DETECT (light heuristic) ----------------------------

_non_ascii = re.compile(r"[^\x00-\x7F]+")
_has_latin = re.compile(r"[A-Za-z]")

def looks_english(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if len(t) < 3:
        return False
    if _non_ascii.search(t):
        # could still be English with accents, but treat as "maybe non-English"
        pass
    return bool(_has_latin.search(t))


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
    return uri.rsplit("/", 1)[-1]

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    q = f"""
    SELECT
      ?polsysLabel
      ?hosLabel ?hosPartyLabel
      ?hogLabel ?hogPartyLabel
      ?legLabel
    WHERE {{
      OPTIONAL {{
        wd:{country_qid} wdt:P122 ?polsys .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P35 ?hos .
        OPTIONAL {{ ?hos wdt:P102 ?hosParty . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P6 ?hog .
        OPTIONAL {{ ?hog wdt:P102 ?hogParty . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}

      OPTIONAL {{
        wd:{country_qid} wdt:P194 ?leg .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      }}
    }}
    LIMIT 80
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    pol_systems = set()
    legislatures = set()

    hos_name = None
    hos_party = None
    hog_name = None
    hog_party = None

    for b in bindings:
        ps = _wd_val(b, "polsysLabel")
        if ps:
            pol_systems.add(ps)

        leg = _wd_val(b, "legLabel")
        if leg:
            legislatures.add(leg)

        if not hos_name:
            hos_name = _wd_val(b, "hosLabel")
        if not hos_party:
            hos_party = _wd_val(b, "hosPartyLabel")

        if not hog_name:
            hog_name = _wd_val(b, "hogLabel")
        if not hog_party:
            hog_party = _wd_val(b, "hogPartyLabel")

    leaders = []
    if hos_name:
        leaders.append({
            "name": hos_name,
            "title": "Head of State",
            "isHeadOfState": True,
            "isHeadOfGovernment": False,
            "party": hos_party,
        })
    if hog_name and hog_name != hos_name:
        leaders.append({
            "name": hog_name,
            "title": "Head of Government",
            "isHeadOfState": False,
            "isHeadOfGovernment": True,
            "party": hog_party,
        })

    executive_party = hog_party or hos_party

    party_control = []
    if executive_party:
        party_control.append({
            "body": "Executive (approx.)",
            "controller": executive_party,
            "controlType": "leader-party",
            "notes": "Derived from leader party; not a seat-count dataset."
        })
    for leg in sorted(legislatures):
        party_control.append({
            "body": leg,
            "controller": "unknown",
            "controlType": "unknown",
            "notes": "Seat/coalition control not reliably available via Wikidata alone."
        })

    skew = "unknown/contested"
    if executive_party and executive_party in PARTY_SKEW_OVERRIDES:
        skew = PARTY_SKEW_OVERRIDES[executive_party]

    political_system = ", ".join(sorted(pol_systems)) if pol_systems else "unknown"

    next_election = {"date": None, "type": None, "notes": "unknown"}
    leader_notes = ""
    if not leaders:
        leader_notes = "No definitive national leader found via Wikidata fields (P35/P6) or data missing."

    return {
        "politicalSystem": political_system,
        "leaders": leaders,
        "leaderNotes": leader_notes,
        "politicalSkewSummary": skew,
        "partyControl": party_control,
        "nextElection": next_election,  # keep as unknown unless you add a specialized election source later
    }


# ---------------------------- NEWS (GDELT) ----------------------------

def _country_queries(country_name: str) -> List[str]:
    return QUERY_ALIASES.get(country_name, [country_name])

def gdelt_recent_articles(country_name: str, max_pull: int = 50) -> List[Dict[str, str]]:
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

    out = []
    seen = set()
    for a in arts:
        title = (a.get("title") or "").strip()
        url = (a.get("url") or "").strip()
        dt = (a.get("seendate") or a.get("date") or "").strip()
        source = (a.get("sourceCommonName") or a.get("source") or a.get("sourceCountry") or "").strip()

        if not title or not url:
            continue
        key = (title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "title": title,
            "url": url,
            "source": source[:80],
            "publishedAt": dt,
            "language": "en",
            "needsTranslation": False,  # sourcelang=English, but leave for UI extension later
        })

    return out

def gdelt_top_stories(country_name: str, n: int = TOP_NEWS_N) -> List[Dict[str, str]]:
    arts = gdelt_recent_articles(country_name, max_pull=60)
    return arts[:n]


# ---------------------------- TRENDS (pytrends) ----------------------------

def _init_pytrends():
    try:
        from pytrends.request import TrendReq  # type: ignore
        proxy = os.getenv("PYTRENDS_PROXY", "").strip()
        proxies = [proxy] if proxy else None
        return TrendReq(hl="en-US", tz=0, proxies=proxies, timeout=(10, 25), retries=0, backoff_factor=0)
    except Exception:
        return None

def trends_top_searches_google(country_iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    pytrends = _init_pytrends()
    if pytrends is None:
        return [], "pytrends_unavailable"

    # today_searches is inconsistent by geo; daily_trends also inconsistent.
    try:
        series = pytrends.today_searches(pn=country_iso2.lower())
        items: List[Dict[str, Any]] = []
        for i, q in enumerate(list(series)[:TOP_TRENDS_N], start=1):
            q = str(q).strip()
            items.append({
                "query": q,
                "rank": i,
                "language": "en" if looks_english(q) else "unknown",
                "needsTranslation": not looks_english(q),
                "source": "google_trends",
            })
        return items, "google_trends_today_searches"
    except Exception:
        try:
            daily = pytrends.daily_trends(country=country_iso2.upper())
            col = "trend" if "trend" in getattr(daily, "columns", []) else None
            vals = daily[col].tolist() if col else daily.iloc[:, 0].tolist()

            items: List[Dict[str, Any]] = []
            for i, q in enumerate([str(x).strip() for x in vals[:TOP_TRENDS_N]], start=1):
                items.append({
                    "query": q,
                    "rank": i,
                    "language": "en" if looks_english(q) else "unknown",
                    "needsTranslation": not looks_english(q),
                    "source": "google_trends",
                })
            return items, "google_trends_daily_trends"
        except Exception:
            return [], "google_trends_unavailable_for_geo"

def _extract_proxy_trends_from_titles(titles: List[str], n: int = TOP_TRENDS_N) -> List[Dict[str, Any]]:
    """
    Fallback when search-trends are unavailable:
    - Extract frequent meaningful tokens/entities from recent English headlines.
    - This is NOT real search data; it's a proxy for "what's being talked about".
    """
    text = " ".join(titles)
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    tokens = [t.lower() for t in text.split() if len(t) >= 4]

    stop = {
        "after","with","from","that","this","they","their","have","will","would",
        "says","said","over","into","amid","more","than","about","what","when",
        "been","were","also","which","who","your","them","today","news",
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
            "language": "en",
            "needsTranslation": False,
            "source": "news_proxy",
            "notes": "Proxy trend (derived from frequent terms in recent English headlines).",
        })
    return out

def trends_top_searches(country_name: str, iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    items, method = trends_top_searches_google(iso2)
    if items:
        return items, method

    # fallback proxy using news
    arts = gdelt_recent_articles(country_name, max_pull=60)
    titles = [a["title"] for a in arts[:25] if a.get("title")]
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

def pick_primary_query(country_name: str) -> str:
    aliases = QUERY_ALIASES.get(country_name)
    return aliases[0] if aliases else country_name

def build_country(country_name: str, iso2: str) -> Dict[str, Any]:
    warnings: List[str] = []
    confidence = 1.0

    gov = {
        "politicalSystem": "unknown",
        "leaders": [],
        "leaderNotes": "",
        "politicalSkewSummary": "unknown/contested",
        "partyControl": [],
        "nextElection": {"date": None, "type": None, "notes": "unknown"},
    }

    qid = get_wikidata_country_qid_by_iso2(iso2)
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
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.20

    trends_items, trends_method = trends_top_searches(country_name, iso2)
    if not trends_items:
        warnings.append("Top searches unavailable; proxy trends also empty.")
        confidence -= 0.10
    elif trends_method.startswith("news_proxy"):
        warnings.append("Top searches are proxy trends (news-derived), not actual search queries.")
        confidence -= 0.05

    try:
        stories = gdelt_top_stories(country_name)
        if len(stories) < TOP_NEWS_N:
            warnings.append("Fewer than 3 recent stories found via GDELT.")
            confidence -= 0.05
    except Exception:
        stories = []
        warnings.append("GDELT fetch failed; news empty.")
        confidence -= 0.15

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
        name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building snapshot for {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2))
        time.sleep(0.2)

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
