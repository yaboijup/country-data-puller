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
- Optional translation: user-provided translation endpoint (e.g., LibreTranslate)

Notes:
- "Party control" is HARD to do universally without seat datasets.
  This script outputs:
    - Executive party (from leader party when available)
    - Legislature bodies listed with controller="unknown" + a note
- "Political skew" is heuristic; mapped when party ideologies are known in overrides.
  Otherwise "unknown/contested" and flagged with warnings.
- English-first: If title/query doesn't look English, we translate (if configured)
  and include original text + disclaimer fields.
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

# Translation (optional): set secrets TRANSLATE_ENDPOINT / TRANSLATE_API_KEY
TRANSLATE_ENDPOINT = os.getenv("TRANSLATE_ENDPOINT", "").strip()
TRANSLATE_API_KEY = os.getenv("TRANSLATE_API_KEY", "").strip()
TRANSLATE_TIMEOUT = 20

# Country list
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


# ---------------------------- SMALL HELPERS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_get(d: dict, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def pick_primary_query(country_name: str) -> str:
    aliases = QUERY_ALIASES.get(country_name)
    return aliases[0] if aliases else country_name

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


# ---------------------------- ENGLISH + TRANSLATION ----------------------------

def looks_english(text: str) -> bool:
    """
    Heuristic: does the text look English-ish?
    """
    if not text:
        return False
    t = text.strip()
    if len(t) < 3:
        return False

    ascii_chars = sum(1 for ch in t if ord(ch) < 128)
    if ascii_chars / max(1, len(t)) < 0.80:
        return False

    if not re.search(r"[A-Za-z]", t):
        return False

    return True

def translate_to_english(text: str, source_lang: str = "auto") -> Tuple[str, Dict[str, Any]]:
    """
    Translate to English using a translation endpoint you provide (e.g., LibreTranslate).
    Returns (translated_text, meta). If translation isn't configured/available, returns original.
    """
    meta: Dict[str, Any] = {
        "translated": False,
        "translationProvider": None,
        "translationNotes": None,
        "original": text,
        "detectedSourceLang": None,
    }

    if not text or looks_english(text):
        return text, meta

    if not TRANSLATE_ENDPOINT:
        meta["translationNotes"] = "Translation skipped (TRANSLATE_ENDPOINT not configured)."
        return text, meta

    payload: Dict[str, Any] = {
        "q": text,
        "source": source_lang,
        "target": "en",
        "format": "text",
    }
    if TRANSLATE_API_KEY:
        payload["api_key"] = TRANSLATE_API_KEY

    try:
        r = requests.post(
            TRANSLATE_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=TRANSLATE_TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json()
            translated = (data.get("translatedText") or "").strip()
            detected = data.get("detectedLanguage") or data.get("detected_language")
            if translated:
                meta["translated"] = True
                meta["translationProvider"] = TRANSLATE_ENDPOINT
                meta["detectedSourceLang"] = detected
                meta["translationNotes"] = "Machine-translated to English."
                return translated, meta
    except Exception:
        pass

    meta["translationNotes"] = "Translation failed (endpoint error)."
    return text, meta

def ensure_english_item(text: str) -> Tuple[str, Dict[str, Any]]:
    return translate_to_english(text, source_lang="auto")


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

def get_wikidata_country_qid(iso2: str) -> Optional[str]:
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

    pol_systems = set()
    legislatures = set()

    hos_name = None
    hos_party = None
    hog_name = None
    hog_party = None
    hos_title = None
    hog_title = None

    for b in bindings:
        ps = _wd_val(b, "polsysLabel")
        if ps:
            pol_systems.add(ps)

        leg = _wd_val(b, "legLabel")
        if leg:
            legislatures.add(leg)

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

    leaders: List[Dict[str, Any]] = []
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

    executive_party = hog_party or hos_party

    party_control: List[Dict[str, Any]] = []
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

    next_election = {"date": None, "type": None, "notes": "unknown"}
    try:
        q2 = f"""
        SELECT ?eLabel ?date WHERE {{
          ?e wdt:P17 wd:{country_qid} .
          ?e wdt:P31/wdt:P279* wd:Q40231 .
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
                next_election = {
                    "date": date_raw[:10],
                    "type": e_label,
                    "notes": "Best-effort from Wikidata; verify with official election bodies."
                }
    except Exception:
        pass

    leader_notes = ""
    if not leaders:
        leader_notes = "No definitive leader found via Wikidata (P35/P6). Possible collective leadership or missing data."

    return {
        "politicalSystem": political_system,
        "leaders": leaders,
        "leaderNotes": leader_notes,
        "politicalSkewSummary": skew,
        "partyControl": party_control,
        "nextElection": next_election,
    }


# ---------------------------- GDELT NEWS ----------------------------

def gdelt_top_stories(country_name: str, max_records: int = TOP_NEWS_N) -> List[Dict[str, Any]]:
    start = (now_utc() - timedelta(days=NEWS_WINDOW_DAYS)).strftime("%Y%m%d%H%M%S")
    end = now_utc().strftime("%Y%m%d%H%M%S")

    queries = QUERY_ALIASES.get(country_name, [country_name])
    q = " OR ".join([f'"{x}"' for x in queries])

    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records * 10),
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
        source = (a.get("sourceCommonName") or a.get("source") or "GDELT").strip()
        dt = (a.get("seendate") or a.get("date") or "").strip()

        if not title or not url:
            continue

        key = (title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)

        final_title, tmeta = ensure_english_item(title)

        out.append({
            "title": final_title,
            "titleOriginal": tmeta["original"],
            "titleWasTranslated": tmeta["translated"],
            "titleTranslationNotes": tmeta["translationNotes"],
            "url": url,
            "source": source[:80],
            "publishedAt": dt,
        })

        if len(out) >= TOP_NEWS_N:
            break

    return out


# ---------------------------- TRENDS (PYTRENDS) ----------------------------

def _init_pytrends():
    try:
        from pytrends.request import TrendReq  # type: ignore
        proxy = os.getenv("PYTRENDS_PROXY", "").strip()
        proxies = [proxy] if proxy else None
        return TrendReq(hl="en-US", tz=0, proxies=proxies, timeout=(10, 25), retries=0, backoff_factor=0)
    except Exception:
        return None

def trends_top_searches(country_iso2: str) -> Tuple[List[Dict[str, Any]], str]:
    pytrends = _init_pytrends()
    if pytrends is None:
        return [], "pytrends unavailable"

    geo = country_iso2.lower()

    # today_searches
    try:
        df = pytrends.today_searches(pn=geo)
        items: List[Dict[str, Any]] = []
        for i, q in enumerate(list(df)[:TOP_TRENDS_N], start=1):
            q_str = str(q).strip()
            q_en, qmeta = ensure_english_item(q_str)
            items.append({
                "query": q_en,
                "queryOriginal": qmeta["original"],
                "queryWasTranslated": qmeta["translated"],
                "queryTranslationNotes": qmeta["translationNotes"],
                "rank": i
            })
        return items, "google_trends_today_searches"
    except Exception:
        pass

    # daily_trends fallback
    try:
        daily = pytrends.daily_trends(country=country_iso2.upper())
        if hasattr(daily, "columns") and "trend" in daily.columns:
            vals = daily["trend"].tolist()
        else:
            vals = daily.iloc[:, 0].tolist()

        items = []
        for i, q in enumerate(vals[:TOP_TRENDS_N], start=1):
            q_str = str(q).strip()
            q_en, qmeta = ensure_english_item(q_str)
            items.append({
                "query": q_en,
                "queryOriginal": qmeta["original"],
                "queryWasTranslated": qmeta["translated"],
                "queryTranslationNotes": qmeta["translationNotes"],
                "rank": i
            })
        return items, "google_trends_daily_trends"
    except Exception:
        return [], "google_trends_unavailable_for_geo"

def us_interest(country_query: str) -> Tuple[Dict[str, Any], str]:
    pytrends = _init_pytrends()
    if pytrends is None:
        return {"query": country_query, "window": "past_24h", "interestIndex": 0, "sparkline": []}, "pytrends unavailable"

    try:
        pytrends.build_payload(kw_list=[country_query], timeframe="now 1-d", geo="US")
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
    gov: Dict[str, Any] = {
        "politicalSystem": "unknown",
        "leaders": [],
        "leaderNotes": "",
        "politicalSkewSummary": "unknown/contested",
        "partyControl": [],
        "nextElection": {"date": None, "type": None, "notes": "unknown"},
    }

    qid = get_wikidata_country_qid(iso2)
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
                warnings.append("Leader fields missing; possible collective leadership or Wikidata gaps.")
                confidence -= 0.10
            ne = gov.get("nextElection", {})
            if not ne or not ne.get("date"):
                warnings.append("Next election not reliably available; verify with official election bodies.")
                confidence -= 0.05
        except Exception:
            warnings.append("Wikidata query error; government fields partially missing.")
            confidence -= 0.20

    # Trends
    trends_items, trends_method = trends_top_searches(iso2)
    if not trends_items:
        warnings.append("Top searches unavailable for this geo via Google Trends (common in CI).")
        confidence -= 0.10
    elif not TRANSLATE_ENDPOINT:
        warnings.append("Translation not configured; non-English trends may appear unmodified.")
        confidence -= 0.03

    # News
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
            "translationEnabled": bool(TRANSLATE_ENDPOINT),
            "translationProvider": TRANSLATE_ENDPOINT or None,
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
        time.sleep(0.2)

    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "countries_snapshot.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote snapshot for {len(out['countries'])} countries to {out_path.resolve()}")

if __name__ == "__main__":
    main()
