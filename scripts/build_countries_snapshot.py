"""
Build a Base44-friendly JSON snapshot for multiple countries.

Outputs: public/countries_snapshot.json

Adds:
- Wikipedia English summary description per country
- Political freedom rating (Freedom House PR+CL 0–100) via OWID grapher CSVs
- Best-effort next national election date (Wikidata upcoming election items)
- Better leader resolution (HoS + HoG; if same person, mark samePerson=true)
- Political system: single label
- Executive controller: leader party (Wikidata)
- Legislature controller (best-effort): winner of most recent national legislative election (Wikidata P1346)
- English-only GDELT titles (hard filtered)

Notes:
- Legislature control is approximate: "winner of most recent legislative election" is not always identical
  to current governing coalition/majority, but is the best globally-automatable method from Wikidata alone.
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

# ENFORCE: only keep English-script headlines for news/proxy trends
ENGLISH_NEWS_ONLY = True

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

# Wikipedia REST summary (English)
WIKI_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"

# Freedom House (via Our World in Data grapher CSVs, updated annually)
# - PR score is 0–40
# - CL score is 0–60
# - total freedom score = PR + CL (0–100)
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


def get_political_system_single(country_qid: str) -> str:
    """
    Return ONE political system label (English). Prefer "preferred rank" if present.
    """
    q = f"""
    SELECT ?polsysLabel WHERE {{
      wd:{country_qid} p:P122 ?stmt .
      ?stmt ps:P122 ?polsys .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?stmtRank)
    LIMIT 1
    """
    # NOTE: stmtRank isn't always bound, but ordering still works harmlessly.
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return "unknown"
    return _wd_val(bindings[0], "polsysLabel") or "unknown"


def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    """
    English labels:
    - political system (single)
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
        leaders.append({
            "name": hos_name,
            "title": "Head of State",
            "party": hos_party,
        })
    if hog_name:
        leaders.append({
            "name": hog_name,
            "title": "Head of Government",
            "party": hog_party,
        })

    # If HoS == HoG, keep both roles but mark samePerson=true
    leader_notes = ""
    if same_person:
        leader_notes = "Head of State and Head of Government are the same person."

    return {
        "leaders": leaders,
        "leadersSamePerson": same_person,
        "leaderNotes": leader_notes,
        "executiveControllerParty": hog_party or hos_party or "unknown",
        "legislatureBodies": sorted(legislatures),
    }


# ---------------------------- ELECTIONS (WIKIDATA, BEST-EFFORT) ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def get_next_national_election(country_qid: str) -> Dict[str, Any]:
    """
    Find the soonest upcoming election item for this jurisdiction (P1001) with a date (P585).

    We filter election types to common national ones (parliamentary/presidential/general/legislative).
    This is best-effort: some countries don't model future elections consistently in Wikidata.
    """
    today = _today_yyyymmdd()

    q = f"""
    SELECT ?e ?eLabel ?date ?typeLabel WHERE {{
      ?e wdt:P1001 wd:{country_qid} .
      ?e wdt:P585 ?date .
      FILTER(?date >= "{today}"^^xsd:dateTime)

      ?e wdt:P31 ?type .
      VALUES ?type {{
        wd:Q40231        # election
        wd:Q152203       # general election
        wd:Q40231        # election (duplicate harmless)
        wd:Q1079032      # parliamentary election
        wd:Q159821       # presidential election
        wd:Q104203       # legislative election
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"hasElections": "unknown", "date": None, "type": None, "name": None, "notes": "No upcoming national election item found in Wikidata."}

    b = bindings[0]
    date = _wd_val(b, "date")
    name = _wd_val(b, "eLabel")
    typ = _wd_val(b, "typeLabel")

    return {
        "hasElections": True,
        "date": date,
        "type": typ,
        "name": name,
        "notes": "Best-effort from Wikidata upcoming election items.",
    }


def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    Approximate legislature control using the winner (P1346) of the most recent legislative/parliamentary/general election.

    This is NOT guaranteed to equal the current seat-majority coalition, but is a defensible automated proxy.
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


def infer_no_elections_from_system(political_system: str) -> Optional[bool]:
    """
    Heuristic: if system label suggests no elections or no meaningful elections.
    Return None if unsure.
    """
    s = (political_system or "").lower()
    triggers = [
        "absolute monarchy",
        "military dictatorship",
        "one-party",
        "one party",
        "totalitarian",
        "junta",
        "theocracy",  # could still have elections, but often restricted; keep as "unknown" unless strong
    ]
    if any(t in s for t in triggers):
        return False
    return None


# ---------------------------- WIKIPEDIA DESCRIPTION ----------------------------

def wikipedia_summary(country_name: str) -> Dict[str, Any]:
    """
    Pull short English description from Wikipedia REST summary.
    """
    title = country_name.replace(" ", "_")
    data = req_json(WIKI_SUMMARY_API + title)
    if not isinstance(data, dict):
        return {"summary": None, "source": "wikipedia", "url": None}

    extract = (data.get("extract") or "").strip()
    url = safe_get(data, "content_urls", "desktop", "page", default=None)

    # Some titles like "UAE" won't resolve well; try primary alias if needed.
    if not extract and country_name in QUERY_ALIASES:
        alt = QUERY_ALIASES[country_name][0].replace(" ", "_")
        data2 = req_json(WIKI_SUMMARY_API + alt)
        extract = (data2.get("extract") or "").strip() if isinstance(data2, dict) else ""
        url = safe_get(data2, "content_urls", "desktop", "page", default=url) if isinstance(data2, dict) else url

    return {"summary": extract or None, "source": "wikipedia", "url": url}


# ---------------------------- FREEDOM HOUSE (OWID CSV) ----------------------------

def _parse_owid_csv(url: str) -> Dict[str, Dict[int, float]]:
    """
    Returns: {country_name: {year: value}}
    OWID grapher CSV is typically: entity, code, year, value
    """
    text = req_text(url)
    if not text:
        return {}
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    out: Dict[str, Dict[int, float]] = {}
    for row in reader:
        entity = (row.get("Entity") or row.get("entity") or "").strip()
        year_str = (row.get("Year") or row.get("year") or "").strip()
        val_str = (row.get("political-rights-score-fh") or row.get("civil-liberties-score-fh") or row.get("Value") or row.get("value") or "").strip()

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
    """
    Compute latest available Freedom House total = PR (0–40) + CL (0–60) => 0–100.
    Status thresholds (Freedom House common usage):
      Free: 70–100
      Partly Free: 35–69
      Not Free: 0–34
    """
    # Try exact name, then alias
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
        # pick latest year where both exist
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
        "components": {"politicalRights": round(float(best_pr), 1), "civilLiberties": round(float(best_cl), 1)},
        "sourceEntityName": best_entity,
        "notes": "Computed as PR(0–40)+CL(0–60) using OWID grapher (Freedom House).",
    }


# ---------------------------- NEWS (GDELT) ----------------------------

def _country_queries(country_name: str) -> List[str]:
    return QUERY_ALIASES.get(country_name, [country_name])

def gdelt_recent_articles(country_name: str, max_pull: int = 80) -> List[Dict[str, Any]]:
    """
    Fetch English-language stories mentioning the country in the last NEWS_WINDOW_DAYS.
    Hard-filters to English-script titles to avoid Mandarin/Cyrillic/etc.
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
    next_elec = {"hasElections": "unknown", "date": None, "type": None, "name": None, "notes": "unknown"}
    leg_control = {"winner": "unknown", "electionName": None, "electionDate": None, "notes": "unknown"}

    if qid:
        try:
            next_elec = get_next_national_election(qid)

            # If Wikidata doesn't list upcoming elections, try heuristic "no elections" for certain systems
            if next_elec.get("hasElections") == "unknown":
                inferred = infer_no_elections_from_system(political_system)
                if inferred is False:
                    next_elec["hasElections"] = False
                    next_elec["notes"] = "Political system suggests no meaningful national elections."

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

    # Freedom rating (replaces skew)
    freedom = freedom_house_rating(country_name, pr_table, cl_table)
    if freedom.get("score") is None:
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

    # News (English only)
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

    # Load Freedom House tables once
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
