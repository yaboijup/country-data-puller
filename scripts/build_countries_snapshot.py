"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

Goal (best-effort, resilient):
- Government snapshot (leaders, political system, legislatures) via Wikidata SPARQL
- "Who controls government bodies" (best-effort):
    - Executive controller approximated by leader party where available
    - Legislature control usually "unknown" unless you add a seat/coalition dataset later
- Next election: often not reliable from Wikidata; included as unknown (with warning)
- Top 3 recent stories about the country (last 3 days) from GDELT DOC 2.1
- Top 10 "search trends" in-country (past ~24h):
    - Try Google Trends (pytrends) best-effort
    - If unavailable (very common for many geos / in CI), fallback = PROXY trends derived from recent English headlines
      (clearly labeled as proxy — not real searches)
- US search interest for the country (past 24h) from Google Trends interest_over_time (best-effort)

Important:
- No translation service required.
- JSON includes flags (languageGuess / needsTranslation) so Base44 can translate at display-time if desired.
"""

from __future__ import annotations

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

WINDOW_HOURS = 24
NEWS_WINDOW_DAYS = 3
TOP_TRENDS_N = 10
TOP_NEWS_N = 3

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

# Your country list
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
QUERY_ALIASES: Dict[str, List[str]] = {
    "UAE": ["United Arab Emirates", "UAE"],
    "United Kingdom": ["United Kingdom", "UK", "Britain"],
    "Palestine": ["Palestine", "Palestinian Territories"],
    "Taiwan": ["Taiwan", "Republic of China"],
    "North Korea": ["North Korea", "DPRK"],
    "South Korea": ["South Korea", "Republic of Korea"],
}

# Political skew mapping is approximate.
PARTY_SKEW_OVERRIDES: Dict[str, str] = {
    "Conservative Party (UK)": "center-right / right",
    "Labour Party (UK)": "center-left",
    "Liberal Democrats (UK)": "center / center-left",
    "Christian Democratic Union of Germany": "center-right",
    "Social Democratic Party of Germany": "center-left",
    "Alliance 90/The Greens": "left / center-left",
    "Liberal Democratic Party (Japan)": "center-right",
}


# ---------------------------- BASIC HELPERS ----------------------------

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

def pick_primary_query(country_name: str) -> str:
    aliases = QUERY_ALIASES.get(country_name)
    return aliases[0] if aliases else country_name


# ---------------------------- LANGUAGE GUESS (light) ----------------------------

def language_guess(text: str) -> str:
    """
    Quick heuristic to tag strings so Base44 can decide to translate.
    """
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
    """
    Resolve the country entity by ISO2 (P297) for stability.
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
    Pull (English labels):
    - political system (P122)
    - head of state (P35) + party (P102)
    - head of government (P6) + party (P102)
    - legislature (P194)

    Party control is best-effort:
      - executive controller = HoG/HoS party
      - legislature controller = unknown (seat data not reliable here)
    """
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
    LIMIT 100
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

    political_system = ", ".join(sorted(pol_systems)) if pol_systems else "unknown"

    executive_party = hog_party or hos_party
    party_control = []
    if executive_party:
        party_control.append({
            "body": "Executive (approx.)",
            "controller": executive_party,
            "controlType": "leader-party",
            "notes": "Derived from leader party; not a seat-count dataset.",
        })
    for leg in sorted(legislatures):
        party_control.append({
            "body": leg,
            "controller": "unknown",
            "controlType": "unknown",
            "notes": "Seat/coalition control not reliably available via Wikidata alone.",
        })

    skew = "unknown/contested"
    if executive_party and executive_party in PARTY_SKEW_OVERRIDES:
        skew = PARTY_SKEW_OVERRIDES[executive_party]

    leader_notes = ""
    if not leaders:
        leader_notes = "No definitive national leader found via Wikidata (P35/P6 missing) or data gaps."

    # Next election is not reliable via Wikidata across countries; keep unknown with warning in quality.
    next_election = {"date": None, "type": None, "notes": "unknown"}

    return {
        "politicalSystem": political_system,
        "leaders": leaders,
        "leaderNotes": leader_notes,
        "politicalSkewSummary": skew,
        "partyControl": party_control,
        "nextElection": next_election,
    }


# ---------------------------- NEWS (GDELT) ----------------------------

def _country_queries(country_name: str) -> List[str]:
    return QUERY_ALIASES.get(country_name, [country_name])

def gdelt_recent_articles(country_name: str, max_pull: int = 60) -> List[Dict[str, Any]]:
    """
    Fetch English-language stories that mention the country name/aliases in the last 3 days.
    NOTE: This is about the country, not necessarily from domestic outlets.
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
            "languageGuess": "en",
            "needsTranslation": False,
        })

    return out

def gdelt_top_stories(country_name: str, n: int = TOP_NEWS_N) -> List[Dict[str, Any]]:
    arts = gdelt_recent_articles(country_name, max_pull=80)
    return arts[:n]


# ---------------------------- TRENDS (pytrends) ----------------------------

def _init_pytrends():
    """
    Lazy import so the whole job doesn't fail if pytrends is blocked.
    """
    try:
        from pytrends.request import TrendReq  # type: ignore
        # Proxies optional; add PYTRENDS_PROXY secret/env if needed
        proxy = (Path(".").joinpath("")).as_posix()  # no-op; keeps lint quiet
        _ = proxy  # unused
        p = None
        proxy_env = os.getenv("PYTRENDS_PROXY", "").strip()
        if proxy_env:
            p = [proxy_env]
        return TrendReq(hl="en-US", tz=0, proxies=p, timeout=(10, 25), retries=0, backoff_factor=0)
    except Exception:
        return None

def trends_top_searches_google(country_iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    """
    Best-effort top searches. This is flaky by geo.
    """
    pytrends = _init_pytrends()
    if pytrends is None:
        return [], "pytrends_unavailable"

    # today_searches expects pn like 'united_states' for some versions, but also accepts country codes in others.
    # We'll try a couple formats.
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

    # daily_trends fallback
    try:
        daily = pytrends.daily_trends(country=country_iso2.upper())
        # column can vary by version
        vals = []
        if hasattr(daily, "columns") and "trend" in daily.columns:
            vals = daily["trend"].tolist()
        elif hasattr(daily, "iloc"):
            vals = daily.iloc[:, 0].tolist()

        items: List[Dict[str, Any]] = []
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
    """
    Fallback when search-trends are unavailable:
    - Extract frequent meaningful tokens from recent English headlines.
    - This is NOT real search data; it's a proxy for "what's being discussed".
    """
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

    # fallback proxy using news
    arts = gdelt_recent_articles(country_name, max_pull=80)
    titles = [a["title"] for a in arts[:25] if a.get("title")]
    proxy = _extract_proxy_trends_from_titles(titles, n=TOP_TRENDS_N)
    return proxy, "news_proxy_trends"


def us_interest(country_query: str) -> Tuple[Dict[str, Any], str]:
    """
    US interest (past 24 hours), best-effort.
    """
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

def build_country(country_name: str, iso2: str) -> Dict[str, Any]:
    warnings: List[str] = []
    confidence = 1.0

    # Government
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
        warnings.append("Wikidata entity not found via ISO2; government fields missing.")
        confidence -= 0.25
    else:
        try:
            gov = get_government_snapshot(qid)
            if gov.get("politicalSkewSummary") == "unknown/contested":
                warnings.append("Political skew is heuristic and may be contested/unavailable.")
                confidence -= 0.05
            if not gov.get("leaders"):
                warnings.append("Leader fields missing (collective leadership or Wikidata gaps).")
                confidence -= 0.10
            # Next election is unknown here by design; warn once.
            warnings.append("Next election is 'unknown' unless you add a dedicated election datasource.")
            confidence -= 0.02
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.20

    # Trends
    trends_items, trends_method = trends_top_searches(country_name, iso2)
    if not trends_items:
        warnings.append("Top searches unavailable; proxy trends also empty.")
        confidence -= 0.10
    elif trends_method.startswith("news_proxy"):
        warnings.append("Top searches are proxy trends (news-derived), not actual search queries.")
        confidence -= 0.05

    # News
    try:
        stories = gdelt_top_stories(country_name)
        if len(stories) < TOP_NEWS_N:
            warnings.append("Fewer than 3 recent stories found via GDELT.")
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
        },
    }

def main():
    out: Dict[str, Any] = {
        "generatedAt": iso_z(now_utc()),
        "windowHours": WINDOW_HOURS,
        "countries": [],
    }

    for c in COUNTRIES:
        name = c["country"]
        iso2 = c["iso2"]
        print(f"▶ Building snapshot for {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2))
        time.sleep(0.25)  # small delay to reduce rate-limit issues

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
