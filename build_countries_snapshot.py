"""
Build a Base44-friendly JSON snapshot for multiple countries.
Output: docs/countries_snapshot.json

Run:  python scripts/build_countries_snapshot.py
Deps: pip install requests beautifulsoup4 lxml

Data strategy (March 2026):
  - Executive names/parties:    STATIC_COUNTRY_DATA (Wikipedia-verified ground truth)
  - Legislature bodies/control: STATIC_COUNTRY_DATA (ground truth)
  - Elections:                  STATIC_COUNTRY_DATA enriched by:
                                  1. IPU Parline API (/v1/parliaments + /v1/elections)
                                  2. ElectionGuide HTML scrape (fallback)
  - Political system:           STATIC_COUNTRY_DATA (cleaned)
  - Metadata:                   REST Countries API (adaptive)
  - Governance:                 World Bank WGI API (adaptive)

Election data model per country:
  elections:
    competitiveElections: bool
    nonCompetitiveReason: str | null
    legislative:
      lastElection:  { date, type, notes }  | null
      nextElection:  { date, type, notes }  | null
      source: str
    executive:
      lastElection:  { date, type, notes }  | null
      nextElection:  { date, type, notes }  | null
      source: str
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("WARNING: beautifulsoup4 not installed. ElectionGuide scraping disabled.")
    print("         Run: pip install beautifulsoup4 lxml")

# ── CONFIG ────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5

WIKIDATA_SPARQL      = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE      = "https://api.worldbank.org/v2"
# IPU Parline v2 API — correct base and endpoints as of 2025
# Docs: https://data.ipu.org/api-doc
IPU_API_BASE         = "https://data.ipu.org"
IPU_PARLIAMENTS_URL  = f"{IPU_API_BASE}/api/parliaments"
IPU_ELECTIONS_URL    = f"{IPU_API_BASE}/api/elections"
REST_COUNTRIES_BASE  = "https://restcountries.com/v3.1"
WIKIPEDIA_API        = "https://en.wikipedia.org/w/api.php"
ELECTIONGUIDE_BASE   = "https://electionguide.org"

WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability":      "VA.PER.RNK",
    "politicalStability":       "PV.PER.RNK",
    "governmentEffectiveness":  "GE.PER.RNK",
    "regulatoryQuality":        "RQ.PER.RNK",
    "ruleOfLaw":                "RL.PER.RNK",
    "controlOfCorruption":      "CC.PER.RNK",
}

WGI_LABEL_TEMPLATES: Dict[str, Dict[str, str]] = {
    "voiceAccountability":     {"Very Low": "Very low voice & accountability",     "Low": "Low voice & accountability",     "Medium": "Moderate voice & accountability",     "High": "High voice & accountability",     "Very High": "Very high voice & accountability"},
    "politicalStability":      {"Very Low": "Very low political stability",         "Low": "Low political stability",         "Medium": "Moderate political stability",         "High": "High political stability",         "Very High": "Very high political stability"},
    "governmentEffectiveness": {"Very Low": "Very low government effectiveness",    "Low": "Low government effectiveness",    "Medium": "Moderate government effectiveness",    "High": "High government effectiveness",    "Very High": "Very high government effectiveness"},
    "regulatoryQuality":       {"Very Low": "Very low regulatory quality",          "Low": "Low regulatory quality",          "Medium": "Moderate regulatory quality",          "High": "High regulatory quality",          "Very High": "Very high regulatory quality"},
    "ruleOfLaw":               {"Very Low": "Very low rule of law",                 "Low": "Low rule of law",                 "Medium": "Moderate rule of law",                 "High": "High rule of law",                 "Very High": "Very high rule of law"},
    "controlOfCorruption":     {"Very Low": "Very low control of corruption",       "Low": "Low control of corruption",       "Medium": "Moderate control of corruption",       "High": "High control of corruption",       "Very High": "Very high control of corruption"},
}
WGI_OVERALL_LABELS: Dict[str, str] = {
    "Very Low":  "Very low governance overall",
    "Low":       "Low governance overall",
    "Medium":    "Moderate governance overall",
    "High":      "High governance overall",
    "Very High": "Very high governance overall",
}

# ── COUNTRY LIST ──────────────────────────────────────────────────────────────

COUNTRIES: List[Dict[str, str]] = [
    {"country": "Russia",         "iso2": "RU"},
    {"country": "India",          "iso2": "IN"},
    {"country": "Pakistan",       "iso2": "PK"},
    {"country": "China",          "iso2": "CN"},
    {"country": "United Kingdom", "iso2": "GB"},
    {"country": "Germany",        "iso2": "DE"},
    {"country": "UAE",            "iso2": "AE"},
    {"country": "Saudi Arabia",   "iso2": "SA"},
    {"country": "Israel",         "iso2": "IL"},
    {"country": "Palestine",      "iso2": "PS"},
    {"country": "Mexico",         "iso2": "MX"},
    {"country": "Brazil",         "iso2": "BR"},
    {"country": "Canada",         "iso2": "CA"},
    {"country": "Nigeria",        "iso2": "NG"},
    {"country": "Japan",          "iso2": "JP"},
    {"country": "Iran",           "iso2": "IR"},
    {"country": "Syria",          "iso2": "SY"},
    {"country": "France",         "iso2": "FR"},
    {"country": "Turkey",         "iso2": "TR"},
    {"country": "Venezuela",      "iso2": "VE"},
    {"country": "Vietnam",        "iso2": "VN"},
    {"country": "Taiwan",         "iso2": "TW"},
    {"country": "South Korea",    "iso2": "KR"},
    {"country": "North Korea",    "iso2": "KP"},
    {"country": "Indonesia",      "iso2": "ID"},
    {"country": "Myanmar",        "iso2": "MM"},
    {"country": "Armenia",        "iso2": "AM"},
    {"country": "Azerbaijan",     "iso2": "AZ"},
    {"country": "Morocco",        "iso2": "MA"},
    {"country": "Somalia",        "iso2": "SO"},
    {"country": "Yemen",          "iso2": "YE"},
    {"country": "Libya",          "iso2": "LY"},
    {"country": "Egypt",          "iso2": "EG"},
    {"country": "Algeria",        "iso2": "DZ"},
    {"country": "Argentina",      "iso2": "AR"},
    {"country": "Chile",          "iso2": "CL"},
    {"country": "Peru",           "iso2": "PE"},
    {"country": "Cuba",           "iso2": "CU"},
    {"country": "Colombia",       "iso2": "CO"},
    {"country": "Panama",         "iso2": "PA"},
    {"country": "El Salvador",    "iso2": "SV"},
    {"country": "Denmark",        "iso2": "DK"},
    {"country": "Sudan",          "iso2": "SD"},
    {"country": "Ukraine",        "iso2": "UA"},
]

# Countries where elections are not competitive / not meaningful to track.
# Value is the human-readable reason shown in output.
NON_COMPETITIVE: Dict[str, str] = {
    "CN": "One-party state. The Chinese Communist Party holds a monopoly on political power. National People's Congress 'elections' are uncontested single-party votes.",
    "KP": "Totalitarian single-party state. Supreme People's Assembly elections feature a single Korean Workers' Party-approved candidate per seat with near-100% reported turnout.",
    "CU": "One-party socialist republic. The Communist Party of Cuba is the only legal party. National Assembly candidates are pre-approved.",
    "VN": "One-party socialist republic. The Communist Party of Vietnam controls all state institutions. National Assembly candidates are vetted by the party.",
    "SA": "Absolute monarchy with no national elections for executive or legislative positions. The Consultative Assembly (Majlis al-Shura) is fully appointed by royal decree.",
    "AE": "Federal constitutional monarchy. Executive positions are hereditary. The Federal National Council is half-appointed, half indirectly elected via a limited electorate.",
    "SY": "Transitional authority following Assad's fall (December 2024). No elections scheduled; country is governed by interim administration (Hayat Tahrir al-Sham).",
    "SD": "Military junta. Sudan is under Sudanese Armed Forces (SAF) control amid civil war with the RSF. No elections scheduled or possible under current conditions.",
    "YE": "Civil war. Two parallel governments (Houthi/Ansar Allah in the north; Presidential Leadership Council in the south). No elections possible.",
    "LY": "Divided state with two rival governments. Elections scheduled for 2021 were never held. No current electoral process.",
    "MM": "Military junta (State Administration Council) since the February 2021 coup. The elected NLD government operates in exile as the National Unity Government.",
    "PS": "No elections held since 2006 (legislative) and 2005 (presidential). Mahmoud Abbas rules by decree; Hamas controls Gaza. Elections indefinitely postponed.",
}

# Countries where IPU data is not applicable (not IPU members or not tracked)
IPU_NOT_APPLICABLE: Dict[str, str] = {
    "TW": "Taiwan is not an IPU member (non-UN member state).",
    "KP": "North Korea holds nominal single-party elections not tracked by IPU as competitive.",
    "CN": "Non-competitive. Not meaningfully tracked by IPU.",
    "CU": "Non-competitive single-party state.",
    "VN": "Non-competitive single-party state.",
    "SA": "No elections. Not in IPU.",
    "AE": "No national elections. Not in IPU.",
    "SY": "No elections. Transitional authority.",
    "SD": "No elections. Military junta.",
    "YE": "No elections. Civil war.",
    "LY": "No elections. Divided state.",
    "MM": "No elections. Military coup.",
    "PS": "No elections. Legislative Council suspended.",
}

# ── STATIC GROUND-TRUTH COUNTRY DATA ─────────────────────────────────────────
# Verified as of March 2026. Wikipedia-confirmed.
# IPU and ElectionGuide enrich election dates at runtime.
#
# elections fields:
#   legislative / executive, each with:
#     lastElection: { date, type, notes }
#     nextElection: { date, type, notes }   — null if unknown/not applicable

STATIC_COUNTRY_DATA: Dict[str, Dict] = {
    "RU": {
        "hosName":  "Vladimir Putin",   "hosParty": "United Russia",
        "hogName":  "Mikhail Mishustin","hogParty": "United Russia",
        "politicalSystem": ["presidential republic", "federal state"],
        "legislature": [
            {"name": "State Duma",        "inControl": "United Russia"},
            {"name": "Federation Council","inControl": "United Russia"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-09-19", "type": "Parliamentary (State Duma)",
                    "notes": "United Russia won 324/450 seats."},
                "nextElection": {"date": "2026-09", "type": "Parliamentary (State Duma)",
                    "notes": "Scheduled September 2026."},
            },
            "executive": {
                "lastElection": {"date": "2024-03-17", "type": "Presidential",
                    "notes": "Putin re-elected with reported 87% of vote."},
                "nextElection": {"date": "2030", "type": "Presidential", "notes": None},
            },
        },
    },
    "IN": {
        "hosName":  "Droupadi Murmu",  "hosParty": "Bharatiya Janata Party",
        "hogName":  "Narendra Modi",   "hogParty": "Bharatiya Janata Party",
        "politicalSystem": ["federal parliamentary democratic republic"],
        "legislature": [
            {"name": "Lok Sabha (lower house)",  "inControl": "BJP-led National Democratic Alliance"},
            {"name": "Rajya Sabha (upper house)", "inControl": "BJP-led National Democratic Alliance"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-04-19", "type": "General election (Lok Sabha)",
                    "notes": "BJP-led NDA won majority; Modi began third term as PM."},
                "nextElection": {"date": "2029", "type": "General election (Lok Sabha)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-07", "type": "Presidential (indirect, parliament)",
                    "notes": "Droupadi Murmu re-confirmed via parliamentary vote."},
                "nextElection": {"date": "2029", "type": "Presidential (indirect)", "notes": None},
            },
        },
    },
    "PK": {
        "hosName":  "Asif Ali Zardari",  "hosParty": "Pakistan Peoples Party",
        "hogName":  "Shehbaz Sharif",    "hogParty": "Pakistan Muslim League (N)",
        "politicalSystem": ["federal parliamentary constitutional republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "PML-N-led coalition"},
            {"name": "Senate",            "inControl": "Coalition government"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-02-08", "type": "General election",
                    "notes": "PTI-backed independents won most seats; PML-N formed coalition government."},
                "nextElection": {"date": "2029", "type": "General election", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-03", "type": "Presidential (indirect)",
                    "notes": "Asif Ali Zardari elected president by parliament."},
                "nextElection": {"date": "2029", "type": "Presidential (indirect)", "notes": None},
            },
        },
    },
    "CN": {
        "hosName":  "Xi Jinping", "hosParty": "Chinese Communist Party",
        "hogName":  "Li Qiang",   "hogParty": "Chinese Communist Party",
        "executiveNote": "Xi Jinping holds supreme authority as General Secretary of the CCP and CMC Chairman. Li Qiang is Premier (head of government).",
        "politicalSystem": ["one-party socialist state"],
        "legislature": [
            {"name": "National People's Congress", "inControl": "Chinese Communist Party"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-03", "type": "NPC delegate selection (non-competitive)",
                    "notes": "NPC delegates selected through party-controlled process. Not a competitive election."},
                "nextElection": {"date": "2028", "type": "NPC delegate selection (non-competitive)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2023-03", "type": "NPC presidential confirmation (non-competitive)",
                    "notes": "Xi Jinping confirmed for third term by NPC; no opposition candidates permitted."},
                "nextElection": {"date": "2028", "type": "NPC confirmation (non-competitive)", "notes": None},
            },
        },
    },
    "GB": {
        "hosName":  "King Charles III",  "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Keir Starmer",      "hogParty": "Labour Party",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy", "unitary state"],
        "legislature": [
            {"name": "House of Commons",  "inControl": "Labour Party"},
            {"name": "House of Lords",    "inControl": "Cross-bench (appointed, non-elected)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-07-04", "type": "General election",
                    "notes": "Labour won a landslide majority (412/650 seats). Keir Starmer became PM."},
                "nextElection": {"date": "2029", "type": "General election",
                    "notes": "Due by January 2029 under the Dissolution and Calling of Parliament Act 2022."},
            },
            "executive": {
                "lastElection": {"date": "2024-07-04", "type": "General election (parliamentary selection of PM)",
                    "notes": "PM is appointed after winning a parliamentary majority."},
                "nextElection": {"date": "2029", "type": "General election", "notes": None},
            },
        },
    },
    "DE": {
        "hosName":  "Frank-Walter Steinmeier", "hosParty": "Social Democratic Party",
        "hogName":  "Friedrich Merz",          "hogParty": "Christian Democratic Union",
        "executiveNote": "CDU/CSU won the February 2025 snap election; Merz became Chancellor in April 2025 after coalition negotiations.",
        "politicalSystem": ["federal parliamentary republic"],
        "legislature": [
            {"name": "Bundestag",  "inControl": "CDU/CSU-led coalition"},
            {"name": "Bundesrat",  "inControl": "State governments (non-partisan body)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2025-02-23", "type": "Federal election (Bundestag)",
                    "notes": "CDU/CSU won 28.5%; SPD 16.4%. AfD came second with 20.8%. Merz formed CDU/CSU-SPD grand coalition."},
                "nextElection": {"date": "2029", "type": "Federal election (Bundestag)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2025-02-23", "type": "Federal election (Chancellor selected by Bundestag)",
                    "notes": "Chancellor is elected by the Bundestag following federal elections."},
                "nextElection": {"date": "2029", "type": "Federal election", "notes": None},
            },
        },
    },
    "AE": {
        "hosName":  "Mohamed bin Zayed Al Nahyan", "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mohammed bin Rashid Al Maktoum","hogParty": "Non-partisan (monarchy)",
        "executiveNote": "UAE is a federal monarchy; executive positions are hereditary. No public elections for president or PM.",
        "politicalSystem": ["federal constitutional monarchy", "absolute monarchy"],
        "legislature": [
            {"name": "Federal National Council", "inControl": "Non-partisan (advisory body; half appointed, half indirectly elected)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-10-07", "type": "Federal National Council (limited indirect election)",
                    "notes": "Only half of the 40-seat FNC is indirectly elected by an appointed electorate of ~300,000. The body is advisory only."},
                "nextElection": {"date": "2027", "type": "Federal National Council (limited indirect)", "notes": None},
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "SA": {
        "hosName":  "King Salman bin Abdulaziz Al Saud", "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mohammed bin Salman",               "hogParty": "Non-partisan (monarchy)",
        "executiveNote": "King Salman is head of state; Crown Prince Mohammed bin Salman (MBS) serves as Prime Minister and is de facto ruler.",
        "politicalSystem": ["absolute monarchy", "theocratic state"],
        "legislature": [
            {"name": "Consultative Assembly (Majlis al-Shura)", "inControl": "Royal appointments (no elections)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": None,
                "nextElection": None,
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "IL": {
        "hosName":  "Isaac Herzog",      "hosParty": "Non-partisan (ceremonial president)",
        "hogName":  "Benjamin Netanyahu","hogParty": "Likud",
        "politicalSystem": ["parliamentary democracy", "unitary republic"],
        "legislature": [
            {"name": "Knesset", "inControl": "Likud-led right-wing coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2022-11-01", "type": "Parliamentary (Knesset)",
                    "notes": "Netanyahu's Likud-led bloc won 64/120 seats."},
                "nextElection": {"date": "2026-10-27", "type": "Parliamentary (Knesset)",
                    "notes": "Legal deadline. Netanyahu may call snap elections earlier (as early as mid-2026)."},
            },
            "executive": {
                "lastElection": {"date": "2022-11-01", "type": "Parliamentary (PM selected by Knesset majority)",
                    "notes": "PM emerges from parliamentary coalition formation after elections."},
                "nextElection": {"date": "2026-10-27", "type": "Parliamentary", "notes": None},
            },
        },
    },
    "PS": {
        "hosName":  "Mahmoud Abbas",    "hosParty": "Fatah",
        "hogName":  "Mohammad Mustafa", "hogParty": "Fatah",
        "executiveNote": "Mahmoud Abbas rules by decree since elections were last held in 2006 (legislative) and 2005 (presidential). PM Mohammad Mustafa was appointed March 2024.",
        "politicalSystem": ["semi-presidential republic", "disputed/occupied territory"],
        "legislature": [
            {"name": "Palestinian Legislative Council", "inControl": "Suspended (no elections since 2006)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2006-01-25", "type": "Legislative Council",
                    "notes": "Hamas won 74/132 seats. No elections held since; council suspended."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": {"date": "2005-01-09", "type": "Presidential",
                    "notes": "Mahmoud Abbas elected. No subsequent presidential election held."},
                "nextElection": None,
            },
        },
    },
    "MX": {
        "hosName":  "Claudia Sheinbaum", "hosParty": "Morena",
        "hogName":  "Claudia Sheinbaum", "hogParty": "Morena",
        "executiveNote": "Mexico has a presidential system. Sheinbaum became Mexico's first elected female president in October 2024.",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Morena-led coalition (supermajority)"},
            {"name": "Senate",              "inControl": "Morena-led coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-06-02", "type": "Congressional (Chamber of Deputies)",
                    "notes": "Morena coalition won supermajority (364/500 seats in Chamber)."},
                "nextElection": {"date": "2027", "type": "Midterm (Chamber of Deputies)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-06-02", "type": "Presidential",
                    "notes": "Claudia Sheinbaum won with ~59% of vote."},
                "nextElection": {"date": "2030", "type": "Presidential",
                    "notes": "Mexico has a single 6-year term (sexenio) with no re-election."},
            },
        },
    },
    "BR": {
        "hosName":  "Luiz Inácio Lula da Silva", "hosParty": "Workers' Party (PT)",
        "hogName":  "Luiz Inácio Lula da Silva", "hogParty": "Workers' Party (PT)",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Centre-right coalition (PL largest party)"},
            {"name": "Federal Senate",       "inControl": "Coalition government (PSD largest bloc)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2022-10-02", "type": "Congressional",
                    "notes": "PL (Bolsonaro's party) won most Chamber seats despite Lula winning presidency."},
                "nextElection": {"date": "2026-10", "type": "Congressional + Presidential",
                    "notes": "Both congressional and presidential elections held simultaneously in October 2026."},
            },
            "executive": {
                "lastElection": {"date": "2022-10-30", "type": "Presidential (runoff)",
                    "notes": "Lula defeated Bolsonaro 50.9% to 49.1% in a runoff."},
                "nextElection": {"date": "2026-10", "type": "Presidential",
                    "notes": "Lula eligible for re-election. October 2026."},
            },
        },
    },
    "CA": {
        "hosName":  "King Charles III (rep. Governor General Mary Simon)", "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mark Carney",  "hogParty": "Liberal Party",
        "executiveNote": "Mark Carney became PM on 14 March 2025 after Trudeau's resignation. Liberals won the 28 April 2025 election with a minority (169 seats).",
        "politicalSystem": ["federal parliamentary constitutional monarchy"],
        "legislature": [
            {"name": "House of Commons", "inControl": "Liberal Party (minority government, 169/343 seats)"},
            {"name": "Senate",           "inControl": "Non-partisan (Independent Senators Group largest bloc)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2025-04-28", "type": "Federal election",
                    "notes": "Liberals won 169/343 seats (minority). Carney's Liberals won amid Trump tariff tensions."},
                "nextElection": {"date": "2029", "type": "Federal election",
                    "notes": "Due by October 2029 at latest; minority government makes earlier election possible."},
            },
            "executive": {
                "lastElection": {"date": "2025-04-28", "type": "Federal election (PM selected by House majority)",
                    "notes": "PM is appointed following federal election result."},
                "nextElection": {"date": "2029", "type": "Federal election", "notes": None},
            },
        },
    },
    "NG": {
        "hosName":  "Bola Tinubu", "hosParty": "All Progressives Congress",
        "hogName":  "Bola Tinubu", "hogParty": "All Progressives Congress",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "All Progressives Congress"},
            {"name": "Senate",                   "inControl": "All Progressives Congress"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-02-25", "type": "General election",
                    "notes": "APC retained majorities in both chambers."},
                "nextElection": {"date": "2027", "type": "General election", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2023-02-25", "type": "Presidential",
                    "notes": "Tinubu won with 36.6% in a fractured three-way race."},
                "nextElection": {"date": "2027", "type": "Presidential", "notes": None},
            },
        },
    },
    "JP": {
        "hosName":  "Emperor Naruhito",  "hosParty": "Non-partisan (imperial household)",
        "hogName":  "Sanae Takaichi",    "hogParty": "Liberal Democratic Party",
        "executiveNote": "Sanae Takaichi became Japan's first female PM in October 2025 after winning the LDP leadership race. She called a snap election on 8 February 2026, winning a historic LDP supermajority.",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "House of Representatives (Shugiin)",   "inControl": "LDP (supermajority — 316/465 seats, Feb 2026)"},
            {"name": "House of Councillors (Sangiin)",        "inControl": "LDP-led coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2026-02-08", "type": "Snap election (House of Representatives)",
                    "notes": "LDP won historic supermajority of 316/465 seats. First snap election called by Takaichi as PM."},
                "nextElection": {"date": "2030", "type": "General election (House of Representatives)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2026-02-08", "type": "Snap election (PM selected by supermajority)",
                    "notes": "PM emerges from the parliamentary majority. Takaichi confirmed."},
                "nextElection": {"date": "2030", "type": "General election", "notes": None},
            },
        },
    },
    "IR": {
        "hosName":  "Interim Leadership Council", "hosParty": "Islamic Republic (transitional)",
        "hogName":  "Masoud Pezeshkian",          "hogParty": "Reformist front",
        "executiveNote": "Ali Khamenei was killed on 28 February 2026 in US/Israeli strikes. A three-member interim council (President, Chief Justice, Guardian Council rep) is managing the transition. A new Supreme Leader election by the Assembly of Experts is expected within 90 days.",
        "politicalSystem": ["theocratic republic", "Islamic republic (transitional)"],
        "legislature": [
            {"name": "Islamic Consultative Assembly (Majlis)", "inControl": "Conservative/Principlist majority"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-03-01", "type": "Parliamentary (Majlis, round 1)",
                    "notes": "Conservatives/Principlists won majority in low-turnout election (~41%). Second round April 2024."},
                "nextElection": {"date": "2028", "type": "Parliamentary (Majlis)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-07-05", "type": "Presidential",
                    "notes": "Masoud Pezeshkian (reformist) won snap election after Raisi's death in helicopter crash."},
                "nextElection": {"date": "2028", "type": "Presidential",
                    "notes": "Assembly of Experts will also elect a new Supreme Leader — timeline uncertain."},
            },
        },
    },
    "SY": {
        "hosName":  "Ahmad al-Sharaa",  "hosParty": "Hayat Tahrir al-Sham (transitional authority)",
        "hogName":  "Mohammad al-Bashir","hogParty": "Transitional government",
        "executiveNote": "Assad fled in December 2024. Ahmad al-Sharaa (formerly Abu Mohammad al-Jolani) leads the transitional government. No functioning legislature.",
        "politicalSystem": ["transitional government (post-civil war)"],
        "legislature": [
            {"name": "No functioning legislature", "inControl": "Transitional authority"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-07-15", "type": "Assad-era parliamentary election (discredited)",
                    "notes": "Final Assad-era election, held during civil war. Not recognised as legitimate."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "FR": {
        "hosName":  "Emmanuel Macron",  "hosParty": "Renaissance",
        "hogName":  "François Bayrou",  "hogParty": "Democratic Movement (MoDem)",
        "executiveNote": "François Bayrou became PM in January 2025 after Michel Barnier's government fell on a no-confidence vote in December 2024.",
        "politicalSystem": ["unitary semi-presidential republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "No single majority (hung parliament; left-wing NFP largest bloc)"},
            {"name": "Senate",            "inControl": "Centre-right (Les Républicains largest group)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-07-07", "type": "Snap legislative election (round 2)",
                    "notes": "Left-wing NFP won most seats but no majority. Macron's Ensemble third. Hung parliament."},
                "nextElection": {"date": "2029", "type": "Legislative",
                    "notes": "Next statutory election due 2029. Snap election possible if government falls again."},
            },
            "executive": {
                "lastElection": {"date": "2022-04-24", "type": "Presidential (runoff)",
                    "notes": "Macron defeated Le Pen 58.5% to 41.5%."},
                "nextElection": {"date": "2027-04", "type": "Presidential",
                    "notes": "Macron is term-limited; cannot run again."},
            },
        },
    },
    "TR": {
        "hosName":  "Recep Tayyip Erdoğan", "hosParty": "Justice and Development Party (AKP)",
        "hogName":  "Recep Tayyip Erdoğan", "hogParty": "Justice and Development Party (AKP)",
        "executiveNote": "Turkey has a presidential system since 2018; Erdoğan is both head of state and government.",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "Grand National Assembly", "inControl": "AKP-led People's Alliance"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-05-14", "type": "Parliamentary + Presidential (simultaneous)",
                    "notes": "AKP and allies retained parliamentary majority."},
                "nextElection": {"date": "2028", "type": "Parliamentary + Presidential", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2023-05-28", "type": "Presidential (runoff)",
                    "notes": "Erdoğan defeated Kılıçdaroğlu 52% to 48% in runoff."},
                "nextElection": {"date": "2028", "type": "Presidential",
                    "notes": "Constitutional two-term limit — Erdoğan may seek a third term via snap election reset."},
            },
        },
    },
    "VE": {
        "hosName":  "Delcy Rodríguez (acting)", "hosParty": "United Socialist Party of Venezuela (PSUV)",
        "hogName":  "Delcy Rodríguez (acting)", "hogParty": "United Socialist Party of Venezuela (PSUV)",
        "executiveNote": "Nicolás Maduro fled Venezuela in late 2025; VP Delcy Rodríguez became acting president on 3 January 2026. Edmundo González is internationally recognised by some states as legitimate president-elect (July 2024 election disputed).",
        "politicalSystem": ["presidential republic (disputed/authoritarian)"],
        "legislature": [
            {"name": "National Assembly", "inControl": "PSUV (government-aligned majority)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2020-12-06", "type": "Parliamentary",
                    "notes": "Opposition boycotted. PSUV won 253/277 seats in discredited election."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": {"date": "2024-07-28", "type": "Presidential",
                    "notes": "Maduro declared winner; results internationally disputed. Opposition's González widely recognised as actual winner."},
                "nextElection": {"date": "2030", "type": "Presidential",
                    "notes": "If current government holds power; legitimacy deeply contested."},
            },
        },
    },
    "VN": {
        "hosName":  "Lương Cường",      "hosParty": "Communist Party of Vietnam",
        "hogName":  "Phạm Minh Chính",  "hogParty": "Communist Party of Vietnam",
        "executiveNote": "Lương Cường became President in October 2024. General Secretary Tô Lâm holds supreme authority as party leader.",
        "politicalSystem": ["one-party socialist republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Communist Party of Vietnam"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-05-23", "type": "National Assembly (non-competitive)",
                    "notes": "All candidates vetted by Vietnam Fatherland Front (CPV body). 99.6% voter turnout reported."},
                "nextElection": {"date": "2026-05", "type": "National Assembly (non-competitive)",
                    "notes": "Scheduled May 2026."},
            },
            "executive": {
                "lastElection": {"date": "2021-07", "type": "Presidential (indirect, National Assembly)",
                    "notes": "President elected by National Assembly on CPV nomination."},
                "nextElection": {"date": "2026", "type": "Presidential (indirect)", "notes": None},
            },
        },
    },
    "TW": {
        "hosName":  "Lai Ching-te",  "hosParty": "Democratic Progressive Party (DPP)",
        "hogName":  "Cho Jung-tai",  "hogParty": "Democratic Progressive Party (DPP)",
        "executiveNote": "Taiwan is not a UN member state; sovereignty disputed by China. Lai Ching-te (William Lai) became president in May 2024.",
        "politicalSystem": ["semi-presidential republic (disputed sovereignty)"],
        "legislature": [
            {"name": "Legislative Yuan", "inControl": "KMT and Taiwan People's Party (TPP) hold combined majority"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-01-13", "type": "Legislative Yuan",
                    "notes": "DPP lost its majority. KMT won most seats; TPP holds balance of power."},
                "nextElection": {"date": "2028", "type": "Legislative Yuan", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-01-13", "type": "Presidential",
                    "notes": "Lai Ching-te won with 40.1% in three-way race."},
                "nextElection": {"date": "2028", "type": "Presidential", "notes": None},
            },
        },
    },
    "KR": {
        "hosName":  "Lee Jae-myung",  "hosParty": "Democratic Party of Korea",
        "hogName":  "Lee Jae-myung",  "hogParty": "Democratic Party of Korea",
        "executiveNote": "Lee Jae-myung won the June 2025 snap presidential election after Yoon Suk-yeol's impeachment and removal for his December 2024 martial law attempt.",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Democratic Party of Korea (supermajority)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-04-10", "type": "Parliamentary",
                    "notes": "Democratic Party won 175/300 seats (majority). Strong rebuke of Yoon government."},
                "nextElection": {"date": "2028", "type": "Parliamentary", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2025-06-03", "type": "Snap presidential election",
                    "notes": "Lee Jae-myung won after Yoon Suk-yeol's impeachment for declaring martial law in December 2024."},
                "nextElection": {"date": "2030", "type": "Presidential",
                    "notes": "South Korea has a single 5-year presidential term with no re-election."},
            },
        },
    },
    "KP": {
        "hosName":  "Kim Jong-un",   "hosParty": "Korean Workers' Party",
        "hogName":  "Kim Jong-un",   "hogParty": "Korean Workers' Party",
        "executiveNote": "Kim Jong-un holds supreme authority as General Secretary of the KWP, President of State Affairs, and Supreme Commander of the armed forces.",
        "politicalSystem": ["one-party totalitarian state", "hereditary dictatorship"],
        "legislature": [
            {"name": "Supreme People's Assembly", "inControl": "Korean Workers' Party (single-party)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-01-07", "type": "SPA election (non-competitive)",
                    "notes": "Single-party slate with a single candidate per constituency. 99.9% turnout reported. Not a competitive election."},
                "nextElection": {"date": "2029", "type": "SPA election (non-competitive)", "notes": None},
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "ID": {
        "hosName":  "Prabowo Subianto", "hosParty": "Gerindra Party",
        "hogName":  "Prabowo Subianto", "hogParty": "Gerindra Party",
        "executiveNote": "Prabowo Subianto became president in October 2024, succeeding Joko Widodo (Jokowi).",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "People's Representative Council (DPR)", "inControl": "Prabowo-allied coalition (majority)"},
            {"name": "Regional Representative Council (DPD)", "inControl": "Non-partisan (regional representatives)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-02-14", "type": "General election (DPR + Presidential simultaneous)",
                    "notes": "Golkar, Gerindra and PDI-P led in DPR seats. Simultaneous with presidential vote."},
                "nextElection": {"date": "2029", "type": "General election", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-02-14", "type": "Presidential",
                    "notes": "Prabowo won 58.6% in a three-candidate race."},
                "nextElection": {"date": "2029", "type": "Presidential",
                    "notes": "Indonesia has a two-term limit; Prabowo eligible for one more term."},
            },
        },
    },
    "MM": {
        "hosName":  "Min Aung Hlaing", "hosParty": "Tatmadaw (military)",
        "hogName":  "Min Aung Hlaing", "hogParty": "Tatmadaw (military)",
        "executiveNote": "Myanmar has been under military junta (State Administration Council) rule since the February 2021 coup. The elected NLD government operates in exile as the National Unity Government.",
        "politicalSystem": ["military junta (State Administration Council)"],
        "legislature": [
            {"name": "Pyidaungsu Hluttaw (suspended)", "inControl": "Dissolved by military coup February 2021"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2020-11-08", "type": "General election (pre-coup)",
                    "notes": "NLD won landslide (396/476 seats). Military nullified results and staged coup on 1 Feb 2021."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": {"date": "2020-11-08", "type": "General election (pre-coup)",
                    "notes": "Last legitimate election before coup."},
                "nextElection": None,
            },
        },
    },
    "AM": {
        "hosName":  "Vahagn Khachaturyan", "hosParty": "Non-partisan (ceremonial)",
        "hogName":  "Nikol Pashinyan",     "hogParty": "Civil Contract",
        "politicalSystem": ["parliamentary republic", "unitary state"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Civil Contract (majority)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-06-20", "type": "Snap parliamentary election",
                    "notes": "Civil Contract won 54% and majority. Called after post-war protests."},
                "nextElection": {"date": "2026", "type": "Parliamentary",
                    "notes": "Due by 2026. Pashinyan may call snap election following Armenia-Azerbaijan peace deal controversy."},
            },
            "executive": {
                "lastElection": {"date": "2022-03-03", "type": "Presidential (indirect, National Assembly)",
                    "notes": "Vahagn Khachaturyan elected by National Assembly. Ceremonial role."},
                "nextElection": {"date": "2027", "type": "Presidential (indirect)", "notes": None},
            },
        },
    },
    "AZ": {
        "hosName":  "Ilham Aliyev",  "hosParty": "New Azerbaijan Party",
        "hogName":  "Ali Asadov",    "hogParty": "New Azerbaijan Party",
        "politicalSystem": ["presidential republic (authoritarian)"],
        "legislature": [
            {"name": "National Assembly (Milli Majlis)", "inControl": "New Azerbaijan Party"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-09-01", "type": "Snap parliamentary election",
                    "notes": "New Azerbaijan Party won 68/100 seats. OSCE reported significant irregularities."},
                "nextElection": {"date": "2029", "type": "Parliamentary", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-02-07", "type": "Snap presidential election",
                    "notes": "Aliyev won 92.1%. No meaningful opposition permitted."},
                "nextElection": {"date": "2031", "type": "Presidential", "notes": None},
            },
        },
    },
    "MA": {
        "hosName":  "King Mohammed VI",   "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Aziz Akhannouch",    "hogParty": "National Rally of Independents (RNI)",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "RNI-led coalition"},
            {"name": "House of Councillors",      "inControl": "RNI-led coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-09-08", "type": "Parliamentary",
                    "notes": "RNI won 102/395 seats and formed a coalition government."},
                "nextElection": {"date": "2026", "type": "Parliamentary",
                    "notes": "Due September 2026."},
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "SO": {
        "hosName":  "Hassan Sheikh Mohamud", "hosParty": "Union for Peace and Development",
        "hogName":  "Hamza Abdi Barre",      "hogParty": "Union for Peace and Development",
        "executiveNote": "Somalia has a fragile federal government with indirect elections via clan delegates. Hassan Sheikh Mohamud was re-elected by parliament in May 2022.",
        "politicalSystem": ["federal parliamentary republic (fragile state)"],
        "legislature": [
            {"name": "People's Assembly (Lower House)", "inControl": "Union for Peace and Development (plurality)"},
            {"name": "Upper House (Senate)",            "inControl": "Coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-10", "type": "Indirect parliamentary election",
                    "notes": "Indirect clan-delegate system. No universal suffrage. Process completed late 2021 / early 2022."},
                "nextElection": {"date": "2026", "type": "Indirect parliamentary election",
                    "notes": "Expected 2025–2026. A move towards direct universal suffrage elections has been discussed but not implemented."},
            },
            "executive": {
                "lastElection": {"date": "2022-05-15", "type": "Presidential (indirect, parliament)",
                    "notes": "Hassan Sheikh Mohamud elected by parliament, 214/328 votes."},
                "nextElection": {"date": "2026", "type": "Presidential (indirect)", "notes": None},
            },
        },
    },
    "YE": {
        "hosName":  "Rashad al-Alimi (Presidential Leadership Council chair)", "hosParty": "Coalition (Presidential Leadership Council)",
        "hogName":  "Ahmed Awad bin Mubarak", "hogParty": "Internationally recognised government",
        "executiveNote": "Yemen is split between Houthi-controlled north and the internationally recognised government. The Presidential Leadership Council (est. 2022) chairs the recognised government.",
        "politicalSystem": ["republic (de facto divided/civil war)"],
        "legislature": [
            {"name": "House of Representatives (suspended)", "inControl": "Divided (parallel governments)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2003-04-27", "type": "Parliamentary",
                    "notes": "Last parliamentary election before civil war. Parliament technically still in session but non-functional."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": {"date": "2012-02-21", "type": "Presidential (single-candidate transition)",
                    "notes": "Abd Rabbuh Mansur Hadi won uncontested election during Arab Spring transition. Replaced by Presidential Leadership Council in 2022."},
                "nextElection": None,
            },
        },
    },
    "LY": {
        "hosName":  "Mohamed al-Menfi (GNU Presidential Council)", "hosParty": "Non-partisan (UN-backed)",
        "hogName":  "Abdul Hamid Dbeibeh (GNU)",                   "hogParty": "Non-partisan",
        "executiveNote": "Libya has two rival governments: the UN-recognised Government of National Unity (GNU) in Tripoli, and the rival government backed by the House of Representatives in Benghazi/Tobruk.",
        "politicalSystem": ["transitional republic (divided/rival governments)"],
        "legislature": [
            {"name": "House of Representatives (HoR)", "inControl": "Eastern-based rival government"},
            {"name": "Government of National Unity (Tripoli)", "inControl": "UN-recognised"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2014-06-25", "type": "Parliamentary",
                    "notes": "Low turnout (18%). Led to political split and parallel governments."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "EG": {
        "hosName":  "Abdel Fattah el-Sisi", "hosParty": "No party (military-backed)",
        "hogName":  "Mostafa Madbouly",     "hogParty": "No party (technocratic)",
        "politicalSystem": ["presidential republic (authoritarian)"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "Pro-Sisi independents and Nation's Future Party"},
            {"name": "Senate",                   "inControl": "Pro-Sisi majority"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2020-10-24", "type": "Parliamentary",
                    "notes": "Nation's Future Party and pro-Sisi independents won overwhelmingly. Opposition largely excluded."},
                "nextElection": {"date": "2025", "type": "Parliamentary",
                    "notes": "Due 2025. Exact date not yet confirmed."},
            },
            "executive": {
                "lastElection": {"date": "2023-12-10", "type": "Presidential",
                    "notes": "Sisi won 89.6% in a tightly controlled election. No meaningful opposition."},
                "nextElection": {"date": "2030", "type": "Presidential", "notes": None},
            },
        },
    },
    "DZ": {
        "hosName":  "Abdelmadjid Tebboune", "hosParty": "National Liberation Front (FLN) aligned",
        "hogName":  "Nadir Larbaoui",       "hogParty": "National Liberation Front (FLN) aligned",
        "politicalSystem": ["presidential republic (dominant-party)"],
        "legislature": [
            {"name": "People's National Assembly", "inControl": "FLN-led coalition"},
            {"name": "Council of the Nation",      "inControl": "FLN-led coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-06-12", "type": "Parliamentary",
                    "notes": "FLN and allies won majority in low-turnout election (~23%)."},
                "nextElection": {"date": "2027", "type": "Parliamentary", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-09-07", "type": "Presidential",
                    "notes": "Tebboune re-elected with 94.7% in a low-turnout election."},
                "nextElection": {"date": "2029", "type": "Presidential", "notes": None},
            },
        },
    },
    "AR": {
        "hosName":  "Javier Milei", "hosParty": "La Libertad Avanza",
        "hogName":  "Javier Milei", "hogParty": "La Libertad Avanza",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "No single majority (Peronist blocs largest; Milei minority)"},
            {"name": "Senate",              "inControl": "No single majority"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-10-22", "type": "Congressional (midterm + presidential)",
                    "notes": "Half of Chamber renewed. Milei's LLA made strong gains; Peronists retained largest bloc."},
                "nextElection": {"date": "2025-10", "type": "Congressional midterm",
                    "notes": "Half of Chamber of Deputies renewed in October 2025."},
            },
            "executive": {
                "lastElection": {"date": "2023-11-19", "type": "Presidential (runoff)",
                    "notes": "Milei defeated Massa (Peronist) 55.7% to 44.3%."},
                "nextElection": {"date": "2027", "type": "Presidential",
                    "notes": "Argentina has a 4-year term with one re-election permitted."},
            },
        },
    },
    "CL": {
        "hosName":  "Gabriel Boric",  "hosParty": "Apruebo Dignidad (Broad Front / PC coalition)",
        "hogName":  "Gabriel Boric",  "hogParty": "Apruebo Dignidad",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Right-wing Chile Vamos coalition (largest bloc)"},
            {"name": "Senate",              "inControl": "Right-wing coalition (majority)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-11-21", "type": "Parliamentary + Presidential (simultaneous)",
                    "notes": "Right-wing Chile Vamos won most seats; Boric's coalition controls government despite congress opposition."},
                "nextElection": {"date": "2025-11", "type": "Parliamentary + Presidential (simultaneous)",
                    "notes": "Boric is term-limited; cannot stand for re-election."},
            },
            "executive": {
                "lastElection": {"date": "2021-12-19", "type": "Presidential (runoff)",
                    "notes": "Gabriel Boric defeated José Antonio Kast 55.9% to 44.1%."},
                "nextElection": {"date": "2025-11", "type": "Presidential",
                    "notes": "Boric cannot run again (single 4-year term). Election November 2025."},
            },
        },
    },
    "PE": {
        "hosName":  "Dina Boluarte", "hosParty": "No active party affiliation",
        "hogName":  "Dina Boluarte", "hogParty": "No active party affiliation",
        "executiveNote": "Dina Boluarte became president in December 2022 after Pedro Castillo's failed self-coup and impeachment. She was previously VP.",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "Congress", "inControl": "Right-wing and centre-right coalition (Alliance for Progress largest bloc)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2021-04-11", "type": "Parliamentary + Presidential (simultaneous)",
                    "notes": "Highly fragmented result. No party won more than 15% of seats."},
                "nextElection": {"date": "2026-04", "type": "Parliamentary + Presidential (simultaneous)",
                    "notes": "April 2026. Boluarte is serving out Castillo's term."},
            },
            "executive": {
                "lastElection": {"date": "2021-06-06", "type": "Presidential (runoff)",
                    "notes": "Castillo defeated Fujimori 50.1% to 49.9%. Castillo impeached Dec 2022; Boluarte assumed presidency."},
                "nextElection": {"date": "2026-04", "type": "Presidential",
                    "notes": "April 2026. Boluarte cannot run (completing prior term, not her own)."},
            },
        },
    },
    "CU": {
        "hosName":  "Miguel Díaz-Canel",    "hosParty": "Communist Party of Cuba",
        "hogName":  "Manuel Marrero Cruz",  "hogParty": "Communist Party of Cuba",
        "politicalSystem": ["one-party socialist republic"],
        "legislature": [
            {"name": "National Assembly of People's Power", "inControl": "Communist Party of Cuba"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2023-03-26", "type": "National Assembly (non-competitive)",
                    "notes": "Single-slate election. All 470 candidates pre-approved by Communist Party."},
                "nextElection": {"date": "2028", "type": "National Assembly (non-competitive)", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2023-04", "type": "Presidential (indirect, National Assembly, non-competitive)",
                    "notes": "Díaz-Canel re-confirmed by National Assembly."},
                "nextElection": {"date": "2028", "type": "Presidential (indirect, non-competitive)", "notes": None},
            },
        },
    },
    "CO": {
        "hosName":  "Gustavo Petro",  "hosParty": "Colombia Humana / Pacto Histórico",
        "hogName":  "Gustavo Petro",  "hogParty": "Pacto Histórico",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "Fragmented (Pacto Histórico minority; no clear majority)"},
            {"name": "Senate",                   "inControl": "Fragmented coalition"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2022-03-13", "type": "Congressional",
                    "notes": "Pacto Histórico won most seats but not a majority."},
                "nextElection": {"date": "2026-03", "type": "Congressional",
                    "notes": "March 2026 — congressional election precedes presidential."},
            },
            "executive": {
                "lastElection": {"date": "2022-06-19", "type": "Presidential (runoff)",
                    "notes": "Petro defeated Hernández 50.4% to 47.3%. First left-wing president in Colombia's history."},
                "nextElection": {"date": "2026-05", "type": "Presidential",
                    "notes": "Petro is term-limited (single 4-year term). May 2026."},
            },
        },
    },
    "PA": {
        "hosName":  "José Raúl Mulino",  "hosParty": "Realizing Goals Party",
        "hogName":  "José Raúl Mulino",  "hogParty": "Realizing Goals Party",
        "politicalSystem": ["presidential republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Fragmented (no single majority; Realizing Goals is largest party)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-05-05", "type": "General election",
                    "notes": "Fragmented result. Mulino's party won plurality in Assembly."},
                "nextElection": {"date": "2029", "type": "General election", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-05-05", "type": "Presidential",
                    "notes": "Mulino won with 34.3% in a six-candidate race. Contested but upheld."},
                "nextElection": {"date": "2029", "type": "Presidential",
                    "notes": "Panama has a single 5-year term with no re-election."},
            },
        },
    },
    "SV": {
        "hosName":  "Nayib Bukele",  "hosParty": "New Ideas",
        "hogName":  "Nayib Bukele",  "hogParty": "New Ideas",
        "executiveNote": "Nayib Bukele was re-elected in February 2024 despite constitutional single-term limits; the Supreme Court approved his candidacy in a controversial ruling.",
        "politicalSystem": ["presidential republic"],
        "legislature": [
            {"name": "Legislative Assembly", "inControl": "New Ideas (supermajority)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2024-02-04", "type": "Presidential + Legislative (simultaneous)",
                    "notes": "New Ideas won 54/60 seats (supermajority). Bukele re-elected with ~85%."},
                "nextElection": {"date": "2027", "type": "Legislative midterm", "notes": None},
            },
            "executive": {
                "lastElection": {"date": "2024-02-04", "type": "Presidential",
                    "notes": "Bukele won ~85% in a non-competitive race after sidelining opposition."},
                "nextElection": {"date": "2030", "type": "Presidential",
                    "notes": "Constitutional status of future terms uncertain given court rulings on term limits."},
            },
        },
    },
    "DK": {
        "hosName":  "King Frederik X",     "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mette Frederiksen",   "hogParty": "Social Democrats",
        "executiveNote": "King Frederik X succeeded Queen Margrethe II on 14 January 2024. PM Frederiksen called a snap election for 24 March 2026, capitalising on popularity from her stance against Trump's Greenland threats.",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "Folketing", "inControl": "Result pending — snap election 24 March 2026"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2022-11-01", "type": "Snap parliamentary election",
                    "notes": "Left-wing bloc won narrow majority. Frederiksen formed minority government."},
                "nextElection": {"date": "2026-03-24", "type": "Snap parliamentary election",
                    "notes": "Called by Frederiksen 4 weeks early amid strong polling on Greenland sovereignty issue."},
            },
            "executive": {
                "lastElection": {"date": "2022-11-01", "type": "Parliamentary (PM selected by Folketing majority)",
                    "notes": "PM emerges from parliamentary majority formation."},
                "nextElection": {"date": "2026-03-24", "type": "Parliamentary", "notes": None},
            },
        },
    },
    "SD": {
        "hosName":  "Abdel Fattah al-Burhan",  "hosParty": "Sudanese Armed Forces (SAF)",
        "hogName":  "Abdel Fattah al-Burhan",  "hogParty": "Sudanese Armed Forces (SAF)",
        "executiveNote": "Sudan has been in civil war since April 2023 between the SAF (al-Burhan) and the Rapid Support Forces (RSF/Hemeti). The civilian transitional framework has collapsed.",
        "politicalSystem": ["military junta (transitional sovereignty council)"],
        "legislature": [
            {"name": "No functioning legislature", "inControl": "Dissolved; civil war ongoing"},
        ],
        "elections": {
            "legislative": {
                "lastElection": None,
                "nextElection": None,
            },
            "executive": {
                "lastElection": None,
                "nextElection": None,
            },
        },
    },
    "UA": {
        "hosName":  "Volodymyr Zelensky",  "hosParty": "Servant of the People",
        "hogName":  "Denys Shmyhal",       "hogParty": "Servant of the People",
        "executiveNote": "Elections are suspended under martial law due to the ongoing Russian invasion. Zelensky's presidential term was extended per wartime provisions; his term legally expired May 2024.",
        "politicalSystem": ["semi-presidential republic (martial law)"],
        "legislature": [
            {"name": "Verkhovna Rada", "inControl": "Servant of the People (majority; elections suspended)"},
        ],
        "elections": {
            "legislative": {
                "lastElection": {"date": "2019-07-21", "type": "Parliamentary",
                    "notes": "Servant of the People won supermajority (254/450 seats). Elections suspended under martial law since 2022."},
                "nextElection": None,
            },
            "executive": {
                "lastElection": {"date": "2019-04-21", "type": "Presidential (runoff)",
                    "notes": "Zelensky defeated Poroshenko 73% to 25%. Elections suspended under martial law; term extended by parliament."},
                "nextElection": None,
            },
        },
    },
}

# ── DATA AVAILABILITY NOTES ───────────────────────────────────────────────────

DATA_AVAILABILITY_NOTES: Dict[str, Dict[str, str]] = {
    "TW": {
        "worldBankGovernance": "Taiwan is not a UN member and is not recognised by the World Bank. WGI data unavailable.",
        "elections": "Taiwan is not an IPU member (non-UN member state). Election data from static records.",
    },
    "PS": {
        "worldBankGovernance": "World Bank data for Palestine is limited due to its political status.",
        "elections": "No elections since 2006 (legislative) and 2005 (presidential). Mahmoud Abbas rules by decree.",
    },
    "KP": {
        "worldBankGovernance": "North Korea governance data is based on limited external assessments.",
        "elections": "North Korea holds nominal single-party elections not tracked by IPU as competitive.",
    },
    "SY": {
        "executive": "Syria's transitional government (post-Assad, December 2024) has no formal electoral basis.",
        "elections": "No elections scheduled. Transitional authority governing.",
    },
    "SO": {
        "worldBankGovernance": "Somalia governance data based on limited external assessments due to conflict.",
    },
    "YE": {
        "worldBankGovernance": "Yemen governance data from internationally recognised government baseline; actual governance severely disrupted by civil war.",
        "elections": "No elections possible. Country divided between Houthi and internationally recognised government.",
    },
    "LY": {
        "worldBankGovernance": "Libya has parallel governing authorities; data reflects the internationally recognised GNU.",
        "elections": "No elections held since 2014. Planned 2021 elections cancelled.",
    },
    "SD": {
        "worldBankGovernance": "Sudan is in active civil war since April 2023; governance data from pre-war assessments.",
        "elections": "No elections. Military junta controls territory contested with RSF.",
    },
    "UA": {
        "elections": "Elections suspended under martial law (Russian invasion ongoing since February 2022).",
    },
    "IR": {
        "executive": "Ali Khamenei killed 28 February 2026. Interim leadership council in place; new Supreme Leader election pending.",
    },
    "VE": {
        "executive": "July 2024 presidential election disputed internationally. Maduro fled; Rodríguez acting president. González recognised by some states as legitimate president-elect.",
    },
    "MM": {
        "elections": "No elections scheduled. Military junta (SAC) suspended parliament after February 2021 coup.",
    },
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(url: str, params: Optional[dict] = None,
             headers: Optional[dict] = None, label: str = "") -> Optional[Any]:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    tag = label or url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                print(f"    [req_json] {tag} → HTTP {r.status_code}")
                return None
            print(f"    [req_json] {tag} → HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            print(f"    [req_json] {tag} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
        _sleep_backoff(attempt)
    print(f"    [req_json] {tag} → all retries exhausted")
    return None

def req_html(url: str, label: str = "") -> Optional[str]:
    """Fetch a URL and return the raw HTML text."""
    h = dict(HEADERS)
    h["Accept"] = "text/html,application/xhtml+xml,*/*;q=0.8"
    tag = label or url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            print(f"    [req_html] {tag} → HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            print(f"    [req_html] {tag} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
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
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {c["iso2"]: c for c in data.get("countries", []) if c.get("iso2")}
    except Exception:
        return {}

# ── QUALITATIVE LABELS ────────────────────────────────────────────────────────

def percentile_to_tier(p: Optional[float]) -> Optional[str]:
    if p is None:
        return None
    if p < 20: return "Very Low"
    if p < 40: return "Low"
    if p < 60: return "Medium"
    if p < 80: return "High"
    return "Very High"

def percentile_to_label(p: Optional[float], dim: str) -> Optional[str]:
    tier = percentile_to_tier(p)
    return WGI_LABEL_TEMPLATES.get(dim, {}).get(tier) if tier else None

def overall_label(p: Optional[float]) -> Optional[str]:
    tier = percentile_to_tier(p)
    return WGI_OVERALL_LABELS.get(tier) if tier else None

# ── WIKIPEDIA ADAPTIVE EXECUTIVE LOOKUP ──────────────────────────────────────

_wiki_exec_cache: Optional[Dict[str, Dict[str, Optional[str]]]] = None

def _load_wiki_exec_cache() -> Dict[str, Dict[str, Optional[str]]]:
    global _wiki_exec_cache
    if _wiki_exec_cache is not None:
        return _wiki_exec_cache

    print("  [WIKI] Fetching Wikipedia heads of state/government list...")

    from html.parser import HTMLParser

    params = {
        "action": "parse",
        "page": "List of current heads of state and government",
        "prop": "text",
        "format": "json",
        "formatversion": "2",
        "disableeditsection": "1",
    }
    data = req_json(WIKIPEDIA_API, params=params, label="Wikipedia HOS/HOG list")
    if not data:
        print("  [WIKI] Failed — using static data only")
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    html_text = safe_get(data, "parse", "text", default="")
    if not html_text:
        _wiki_exec_cache = {}
        return _wiki_exec_cache

    result: Dict[str, Dict[str, Optional[str]]] = {}

    class TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.in_cell = False
            self.current_row: List[str] = []
            self.current_cell_parts: List[str] = []
            self.rows: List[List[str]] = []

        def _cell_text(self) -> str:
            raw = " ".join(self.current_cell_parts).strip()
            raw = re.sub(r"\[\d+\]", "", raw)
            raw = re.sub(r"\s+", " ", raw).strip()
            return raw

        def handle_starttag(self, tag, attrs):
            attrs_dict = dict(attrs)
            if tag == "table" and "wikitable" in attrs_dict.get("class", ""):
                self.in_table = True
            if not self.in_table:
                return
            if tag == "tr":
                self.current_row = []
            if tag in ("td", "th"):
                self.in_cell = True
                self.current_cell_parts = []
            if tag == "br":
                self.current_cell_parts.append(" | ")

        def handle_endtag(self, tag):
            if not self.in_table:
                return
            if tag in ("td", "th") and self.in_cell:
                self.current_row.append(self._cell_text())
                self.in_cell = False
                self.current_cell_parts = []
            if tag == "tr" and self.current_row:
                self.rows.append(self.current_row)
                self.current_row = []
            if tag == "table":
                self.in_table = False

        def handle_data(self, data):
            if self.in_cell:
                self.current_cell_parts.append(data)

        def handle_entityref(self, name):
            if self.in_cell:
                entities = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ",
                            "ndash": "–", "mdash": "—"}
                self.current_cell_parts.append(entities.get(name, ""))

        def handle_charref(self, name):
            if self.in_cell:
                try:
                    c = chr(int(name[1:], 16) if name.startswith("x") else int(name))
                    self.current_cell_parts.append(c)
                except Exception:
                    pass

    parser = TableParser()
    parser.feed(html_text)

    WIKI_NAME_MAP = {
        "RU": "Russia", "IN": "India", "PK": "Pakistan", "CN": "China",
        "GB": "United Kingdom", "DE": "Germany", "AE": "United Arab Emirates",
        "SA": "Saudi Arabia", "IL": "Israel", "PS": "Palestine",
        "MX": "Mexico", "BR": "Brazil", "CA": "Canada", "NG": "Nigeria",
        "JP": "Japan", "IR": "Iran", "SY": "Syria", "FR": "France",
        "TR": "Turkey", "VE": "Venezuela", "VN": "Vietnam",
        "KR": "South Korea", "KP": "North Korea", "ID": "Indonesia",
        "MM": "Myanmar", "AM": "Armenia", "AZ": "Azerbaijan", "MA": "Morocco",
        "SO": "Somalia", "YE": "Yemen", "LY": "Libya", "EG": "Egypt",
        "DZ": "Algeria", "AR": "Argentina", "CL": "Chile", "PE": "Peru",
        "CU": "Cuba", "CO": "Colombia", "PA": "Panama", "SV": "El Salvador",
        "DK": "Denmark", "SD": "Sudan", "UA": "Ukraine",
    }
    rev = {v.lower(): k for k, v in WIKI_NAME_MAP.items()}

    def _first_name(s: str) -> Optional[str]:
        if not s:
            return None
        parts = [p.strip() for p in s.split("|") if p.strip()]
        return (parts[0] if parts else s).strip() or None

    for row in parser.rows:
        if len(row) < 2:
            continue
        country_raw = row[0].strip()
        if not country_raw or country_raw.lower() in ("country", "state", ""):
            continue
        iso2 = rev.get(country_raw.lower())
        if not iso2:
            for wiki_lower, code in rev.items():
                if wiki_lower in country_raw.lower() or country_raw.lower() in wiki_lower:
                    iso2 = code
                    break
        if not iso2:
            continue
        hos_raw = row[1].strip() if len(row) > 1 else ""
        hog_raw = row[2].strip() if len(row) > 2 else hos_raw
        result[iso2] = {
            "hosName": _first_name(hos_raw),
            "hogName": _first_name(hog_raw),
        }

    print(f"  [WIKI] Parsed {len(result)} countries")
    _wiki_exec_cache = result
    return result

# ── IPU PARLINE (correct v2 API) ──────────────────────────────────────────────
# IPU Parline API documentation: https://data.ipu.org/api-doc
# The correct approach is:
#   GET /api/parliaments          → list all parliaments with their IDs
#   GET /api/elections?...        → filter elections by parliament ID
# The old /v1/chambers/{ISO2} endpoint no longer exists.

_ipu_parliament_map: Optional[Dict[str, Dict]] = None  # iso2 → parliament record

def _load_ipu_parliament_map() -> Dict[str, Dict]:
    """
    Load the full IPU parliament list once and build a lookup by ISO2.
    IPU uses its own parliament IDs, so we need this map to query elections.
    """
    global _ipu_parliament_map
    if _ipu_parliament_map is not None:
        return _ipu_parliament_map

    print("  [IPU] Loading parliament list from IPU Parline API...")
    _ipu_parliament_map = {}

    # Try the v2 parliaments endpoint
    # IPU API returns paginated JSON; iterate pages
    page = 1
    per_page = 100
    total_loaded = 0

    while True:
        params = {"page": page, "per_page": per_page, "format": "json"}
        data = req_json(
            f"{IPU_API_BASE}/api/parliaments",
            params=params,
            headers={"Accept": "application/json"},
            label=f"IPU /api/parliaments page {page}",
        )
        if not data:
            print(f"  [IPU] Failed to load parliament list page {page}")
            break

        # Handle both list and paginated dict response
        records: List[Dict] = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data") or data.get("results") or data.get("parliaments") or []
            # Check if this is the only page
            if not records and "id" in data:
                records = [data]

        if not records:
            print(f"  [IPU] No records on page {page}, stopping pagination")
            break

        for rec in records:
            if not isinstance(rec, dict):
                continue
            # Extract ISO2 from various possible fields
            country = rec.get("country") or rec.get("countryCode") or {}
            iso2 = None
            if isinstance(country, dict):
                iso2 = (country.get("isoCode") or country.get("iso2") or
                        country.get("code") or "").upper()
            elif isinstance(country, str):
                iso2 = country.upper()
            if not iso2:
                iso2 = (rec.get("isoCode") or rec.get("iso2") or
                        rec.get("country_code") or "").upper()
            if iso2 and len(iso2) == 2:
                if iso2 not in _ipu_parliament_map:
                    _ipu_parliament_map[iso2] = rec
                total_loaded += 1

        print(f"  [IPU] Page {page}: loaded {len(records)} records, {len(_ipu_parliament_map)} unique countries so far")

        # If fewer records than per_page, we've hit the last page
        if len(records) < per_page:
            break

        page += 1
        time.sleep(0.3)

    print(f"  [IPU] Parliament map loaded: {len(_ipu_parliament_map)} countries")

    # Log a sample to understand the data shape
    if _ipu_parliament_map:
        sample_iso2 = next(iter(_ipu_parliament_map))
        sample = _ipu_parliament_map[sample_iso2]
        print(f"  [IPU] Sample record ({sample_iso2}) keys: {list(sample.keys())[:15]}")

    return _ipu_parliament_map


def _get_ipu_elections_for_country(iso2: str) -> List[Dict]:
    """
    Fetch recent and upcoming elections for a country from IPU.
    Returns a list of election records sorted by date descending.
    """
    parl_map = _load_ipu_parliament_map()
    parl = parl_map.get(iso2.upper())

    if not parl:
        return []

    # Try to get parliament ID in various formats
    parl_id = parl.get("id") or parl.get("parliamentId") or parl.get("parliament_id")
    if not parl_id:
        return []

    print(f"    [IPU] Fetching elections for {iso2} (parliament ID: {parl_id})")

    # Query elections endpoint
    params = {
        "parliament": parl_id,
        "format": "json",
        "per_page": 20,
        "sort": "date_desc",
    }
    data = req_json(
        f"{IPU_API_BASE}/api/elections",
        params=params,
        headers={"Accept": "application/json"},
        label=f"IPU /api/elections?parliament={parl_id}",
    )

    if not data:
        return []

    records: List[Dict] = []
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get("data") or data.get("results") or data.get("elections") or []

    return [r for r in records if isinstance(r, dict)]


def _parse_ipu_date(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("date") or raw.get("text")
    if not raw:
        return None
    s = str(raw).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s): return s
    if re.match(r"^\d{4}-\d{2}$", s):        return s
    if re.match(r"^\d{4}$", s):               return s
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T", s)
    if m: return m.group(1)
    return s or None


def _extract_ipu_election_date(rec: Dict) -> Optional[str]:
    """Try multiple field names for the election date in an IPU record."""
    for field in ("date", "electionDate", "election_date", "dateOfElection",
                  "lastElectionDate", "date_of_election"):
        val = rec.get(field)
        if val:
            d = _parse_ipu_date(val)
            if d:
                return d
    return None


def fetch_ipu_elections(iso2: str) -> Dict[str, Any]:
    """
    Returns a dict:
      lastDate: str | None
      nextDate: str | None
      elections: list of raw election records (for debugging)
      source: str
      notes: str
    """
    if iso2.upper() in IPU_NOT_APPLICABLE:
        reason = IPU_NOT_APPLICABLE[iso2.upper()]
        return {"lastDate": None, "nextDate": None, "elections": [],
                "source": "ipu_not_applicable", "notes": reason}

    elections = _get_ipu_elections_for_country(iso2)
    if not elections:
        return {"lastDate": None, "nextDate": None, "elections": [],
                "source": "ipu_no_data",
                "notes": f"IPU Parline returned no elections for {iso2}."}

    today = datetime.now(timezone.utc).date()
    past_dates: List[str] = []
    future_dates: List[str] = []

    for rec in elections:
        d = _extract_ipu_election_date(rec)
        if not d:
            continue
        # Parse to compare with today
        try:
            # Handle partial dates (YYYY-MM, YYYY)
            if len(d) == 4:
                dt = datetime(int(d), 12, 31).date()
            elif len(d) == 7:
                y, m = d.split("-")
                dt = datetime(int(y), int(m), 28).date()
            else:
                dt = datetime.strptime(d, "%Y-%m-%d").date()

            if dt <= today:
                past_dates.append(d)
            else:
                future_dates.append(d)
        except ValueError:
            past_dates.append(d)

    last_date = max(past_dates) if past_dates else None
    next_date = min(future_dates) if future_dates else None

    return {
        "lastDate": last_date,
        "nextDate": next_date,
        "elections": elections[:5],  # keep a few for debugging
        "source": "ipu_parline",
        "notes": f"IPU Parline: {len(elections)} election record(s) found.",
    }

# ── ELECTIONGUIDE SCRAPER (fallback enrichment) ───────────────────────────────

_eg_cache: Optional[Dict[str, List[Dict]]] = None  # iso2 → list of election dicts

def _load_electionguide_cache() -> Dict[str, List[Dict]]:
    """
    Scrape ElectionGuide's past and upcoming election pages.
    Returns a dict mapping country names → list of {date, body, url, status}.
    We then cross-reference with our COUNTRIES list by name.
    """
    global _eg_cache
    if _eg_cache is not None:
        return _eg_cache

    _eg_cache = {}

    if not BS4_AVAILABLE:
        print("  [EG] beautifulsoup4 not available, skipping ElectionGuide scrape")
        return _eg_cache

    # Map ElectionGuide country names → ISO2
    # ElectionGuide uses its own country naming; these are the common mismatches
    EG_NAME_OVERRIDES: Dict[str, str] = {
        "United Kingdom of Great Britain and Northern Ireland": "GB",
        "United Arab Emirates": "AE",
        "Korea, Republic of": "KR",
        "Korea (North)": "KP",
        "Korea, Democratic People's Republic of": "KP",
        "Viet Nam": "VN",
        "Vietnam": "VN",
        "Iran, Islamic Republic of": "IR",
        "Syrian Arab Republic": "SY",
        "Bolivia, Plurinational State of": "BO",
        "Venezuela, Bolivarian Republic of": "VE",
        "Congo (Brazzaville)": "CG",
        "Congo, Democratic Republic of the": "CD",
        "Türkiye": "TR",
        "Turkey": "TR",
        "Russian Federation": "RU",
        "Republic of Korea": "KR",
    }

    # Build reverse map from our COUNTRIES list
    country_name_to_iso2: Dict[str, str] = {c["country"].lower(): c["iso2"] for c in COUNTRIES}
    # Add official name overrides
    for eg_name, iso2 in EG_NAME_OVERRIDES.items():
        country_name_to_iso2[eg_name.lower()] = iso2

    def _name_to_iso2(name: str) -> Optional[str]:
        clean = name.strip().lower()
        if clean in country_name_to_iso2:
            return country_name_to_iso2[clean]
        # Partial match fallback
        for known, code in country_name_to_iso2.items():
            if known in clean or clean in known:
                return code
        return None

    def _parse_eg_page(url: str, status: str) -> None:
        """Parse an ElectionGuide listing page and populate _eg_cache."""
        print(f"  [EG] Scraping {url}")
        html = req_html(url, label=f"ElectionGuide {status}")
        if not html:
            print(f"  [EG] Failed to fetch {url}")
            return

        soup = BeautifulSoup(html, "lxml")

        # ElectionGuide election listings are in <table> rows or structured divs
        # Each row typically has: flag, date+body name, country name
        # The structure as of 2025: rows with class pattern or just td structure
        parsed_count = 0

        # Try table rows first
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # Extract date — usually first or second cell with date text
            date_text = ""
            body_text = ""
            country_text = ""

            for cell in cells:
                text = cell.get_text(separator=" ", strip=True)
                # Date pattern: "Mar 24 2026" or "Mar 24 2026 (d)"
                date_m = re.search(
                    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4}",
                    text,
                )
                if date_m and not date_text:
                    date_text = date_m.group(0)
                # Links give us body name and country
                for a in cell.find_all("a"):
                    href = a.get("href", "")
                    link_text = a.get_text(strip=True)
                    if "/elections/id/" in href and link_text and not body_text:
                        body_text = link_text
                    elif "/countries/id/" in href and link_text and not country_text:
                        country_text = link_text

            if not (date_text and country_text):
                continue

            # Parse date to ISO format
            try:
                dt = datetime.strptime(date_text, "%b %d %Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                iso_date = date_text

            iso2 = _name_to_iso2(country_text)
            if not iso2:
                continue

            if iso2 not in _eg_cache:
                _eg_cache[iso2] = []
            _eg_cache[iso2].append({
                "date": iso_date,
                "body": body_text,
                "country": country_text,
                "status": status,
            })
            parsed_count += 1

        print(f"  [EG] Parsed {parsed_count} elections from {url}")
        time.sleep(0.5)

    _parse_eg_page(f"{ELECTIONGUIDE_BASE}/elections/type/past/", "past")
    _parse_eg_page(f"{ELECTIONGUIDE_BASE}/elections/type/upcoming/", "upcoming")

    total = sum(len(v) for v in _eg_cache.values())
    print(f"  [EG] Cache complete: {total} elections across {len(_eg_cache)} countries")
    return _eg_cache


def get_electionguide_dates(iso2: str) -> Dict[str, Optional[str]]:
    """
    Returns best lastDate / nextDate from ElectionGuide for the given iso2.
    """
    cache = _load_electionguide_cache()
    records = cache.get(iso2.upper(), [])
    if not records:
        return {"lastDate": None, "nextDate": None, "source": "electionguide_no_data"}

    today = datetime.now(timezone.utc).date()
    past: List[str] = []
    future: List[str] = []

    for rec in records:
        d = rec.get("date", "")
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            if dt <= today:
                past.append(d)
            else:
                future.append(d)
        except ValueError:
            past.append(d)

    return {
        "lastDate": max(past) if past else None,
        "nextDate": min(future) if future else None,
        "source": "electionguide",
    }

# ── REST COUNTRIES ────────────────────────────────────────────────────────────

def fetch_rest_countries(iso2: str) -> Dict[str, Any]:
    url = f"{REST_COUNTRIES_BASE}/alpha/{iso2.lower()}"
    data = req_json(url, label=f"REST Countries /alpha/{iso2}")

    if isinstance(data, list):
        data = data[0] if data else None
    elif not isinstance(data, dict):
        data = None

    if not data:
        return {
            "capital": None, "population": None, "region": None, "subregion": None,
            "flag": None, "flagPng": None, "currencies": [], "languages": [],
            "officialName": None, "source": "restcountries",
        }

    cap_raw = data.get("capital")
    capital = cap_raw[0] if isinstance(cap_raw, list) and cap_raw else None

    curr_raw = data.get("currencies") or {}
    currencies = [v["name"] for v in curr_raw.values()
                  if isinstance(v, dict) and v.get("name")]

    lang_raw = data.get("languages") or {}
    languages = list(lang_raw.values()) if isinstance(lang_raw, dict) else []

    name_obj = data.get("name") or {}
    official = name_obj.get("official") if isinstance(name_obj, dict) else None

    flags = data.get("flags") or {}
    flag_png = flags.get("png") if isinstance(flags, dict) else None

    return {
        "capital":      capital,
        "population":   data.get("population"),
        "region":       data.get("region"),
        "subregion":    data.get("subregion"),
        "flag":         data.get("flag"),
        "flagPng":      flag_png,
        "currencies":   currencies,
        "languages":    languages,
        "officialName": official,
        "source":       "restcountries",
    }

# ── WORLD BANK WGI ────────────────────────────────────────────────────────────

def _parse_wb(payload: Any) -> Tuple[Optional[float], Optional[int], Optional[str]]:
    if (not isinstance(payload, list) or len(payload) < 2
            or not isinstance(payload[1], list)):
        return None, None, "Unexpected WB response shape."
    for row in payload[1]:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        dt  = row.get("date")
        if val is None or dt is None:
            continue
        try:
            return float(val), int(dt), None
        except Exception:
            continue
    return None, None, "No non-null value in WB series."

def fetch_wgi(iso2: str) -> Dict[str, Any]:
    components: Dict[str, Any] = {}
    years:  List[int]   = []
    values: List[float] = []
    sources: Dict[str, str] = {}

    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        url = f"{WORLD_BANK_BASE}/country/{iso2.lower()}/indicator/{code}"
        payload = req_json(url, params={"format": "json", "per_page": 60},
                           label=f"WB {code} {iso2}")
        sources[dim] = url
        if payload is None:
            components[dim] = {"indicator": code, "percentile": None, "label": None,
                                "year": None, "notes": "Failed to fetch WB indicator."}
            continue
        v, y, notes = _parse_wb(payload)
        if v is not None and y is not None:
            components[dim] = {"indicator": code, "percentile": v,
                                "label": percentile_to_label(v, dim), "year": y}
            values.append(v)
            years.append(y)
        else:
            components[dim] = {"indicator": code, "percentile": None, "label": None,
                                "year": None, "notes": notes}

    if not values:
        return {"ok": False, "overallPercentile": None, "band": "unknown",
                "bandLabel": None, "year": None, "components": components,
                "sources": sources, "notes": "No WGI percentile values available."}

    overall = sum(values) / len(values)
    return {"ok": True, "overallPercentile": round(overall, 2),
            "band": percentile_to_tier(overall), "bandLabel": overall_label(overall),
            "year": max(years), "components": components, "sources": sources,
            "notes": None}

def merge_wb_sticky(new_wb: Dict, prev: Optional[Dict]) -> Dict:
    prev_wb = (prev or {}).get("worldBankGovernance") if isinstance(prev, dict) else None
    if new_wb.get("ok"):
        out = dict(new_wb)
        out.pop("ok", None)
        return out
    if isinstance(prev_wb, dict) and prev_wb.get("overallPercentile") is not None:
        kept = dict(prev_wb)
        kept["notes"] = f"Kept previous values; latest fetch failed: {new_wb.get('notes')}"
        return kept
    out = dict(new_wb)
    out.pop("ok", None)
    return out

# ── BUILD ELECTION OBJECT ─────────────────────────────────────────────────────

def _election_obj(date: Optional[str], etype: Optional[str],
                  notes: Optional[str]) -> Optional[Dict]:
    """Create a structured election object, or None if no date."""
    if date is None and etype is None:
        return None
    return {"date": date, "type": etype, "notes": notes}


def build_elections_block(iso2: str, static: Dict,
                           ipu: Dict, eg: Dict) -> Dict[str, Any]:
    """
    Merge static ground truth with IPU and ElectionGuide enrichment.
    Priority: static dates (ground truth) > IPU > ElectionGuide.
    IPU/EG are used to confirm or fill in gaps, never to override verified static data.

    Returns the full elections block including competitiveElections flag.
    """
    is_non_competitive = iso2 in NON_COMPETITIVE
    non_competitive_reason = NON_COMPETITIVE.get(iso2)

    static_elec = static.get("elections") or {}
    static_leg  = static_elec.get("legislative") or {}
    static_exec = static_elec.get("executive") or {}

    static_leg_last = static_leg.get("lastElection")
    static_leg_next = static_leg.get("nextElection")
    static_exec_last = static_exec.get("lastElection")
    static_exec_next = static_exec.get("nextElection")

    # For legislative next date: use static as truth, fill from IPU/EG only if static has no next
    leg_next = static_leg_next
    leg_next_source = "static_ground_truth"

    if leg_next is None and not is_non_competitive:
        # Try IPU enrichment
        ipu_next = ipu.get("nextDate")
        if ipu_next:
            leg_next = _election_obj(
                date=ipu_next,
                etype="Parliamentary (IPU Parline)",
                notes=ipu.get("notes"),
            )
            leg_next_source = "ipu_parline"
        else:
            # Try ElectionGuide
            eg_next = eg.get("nextDate")
            if eg_next:
                leg_next = _election_obj(
                    date=eg_next,
                    etype="Parliamentary (ElectionGuide)",
                    notes="Date sourced from ElectionGuide. Verify type.",
                )
                leg_next_source = "electionguide"

    # Determine election source description
    if ipu.get("source") == "ipu_parline":
        leg_source = "static_ground_truth + ipu_parline enrichment"
    elif eg.get("source") == "electionguide":
        leg_source = "static_ground_truth + electionguide enrichment"
    else:
        leg_source = "static_ground_truth"

    legislative = {
        "lastElection": static_leg_last,
        "nextElection": leg_next,
        "source": leg_source,
    }

    executive = {
        "lastElection": static_exec_last,
        "nextElection": static_exec_next,
        "source": "static_ground_truth",
    }

    return {
        "competitiveElections": not is_non_competitive,
        "nonCompetitiveReason": non_competitive_reason,
        "legislative": legislative,
        "executive": executive,
    }

# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def build_country(name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
    static = STATIC_COUNTRY_DATA.get(iso2, {})
    if not static:
        print(f"  [{iso2}] WARNING: no static data — output will be sparse")

    # ── Executive ─────────────────────────────────────────────────────────────
    # Static always wins for complex/transitional situations
    STATIC_WINS = {"IR", "SY", "VE", "KR", "JP", "TW", "MM", "KP", "SD", "YE", "LY", "PS"}

    print(f"  [{iso2}] Wikipedia executive lookup...")
    wiki = _load_wiki_exec_cache().get(iso2, {})

    if iso2 in STATIC_WINS or not wiki.get("hosName"):
        hos_name = static.get("hosName")
        hog_name = static.get("hogName")
        exec_source = "static_ground_truth"
    else:
        hos_name = wiki.get("hosName") or static.get("hosName")
        hog_name = wiki.get("hogName") or static.get("hogName")
        exec_source = "wikipedia:List_of_current_heads_of_state_and_government"

    hos_party  = static.get("hosParty", "unknown")
    hog_party  = static.get("hogParty", "unknown")
    exec_note  = static.get("executiveNote")
    pol_sys    = static.get("politicalSystem", ["unknown"])

    print(f"  [{iso2}] HOS={hos_name}, HOG={hog_name}")

    # ── Legislature ───────────────────────────────────────────────────────────
    leg_static = static.get("legislature", [{"name": "Legislature", "inControl": "unknown"}])
    legislature = [
        {
            "name":          b["name"],
            "inControl":     b.get("inControl", "unknown"),
            "controlMethod": "static_ground_truth",
        }
        for b in leg_static
    ]

    # ── Elections enrichment (IPU + ElectionGuide) ────────────────────────────
    print(f"  [{iso2}] IPU elections fetch...")
    ipu = fetch_ipu_elections(iso2)
    print(f"  [{iso2}] IPU: lastDate={ipu.get('lastDate')}, nextDate={ipu.get('nextDate')}, src={ipu.get('source')}")

    print(f"  [{iso2}] ElectionGuide date lookup...")
    eg = get_electionguide_dates(iso2)
    print(f"  [{iso2}] EG: lastDate={eg.get('lastDate')}, nextDate={eg.get('nextDate')}")

    elections = build_elections_block(iso2, static, ipu, eg)

    # ── World Bank WGI ────────────────────────────────────────────────────────
    print(f"  [{iso2}] WB WGI fetch...")
    new_wb = fetch_wgi(iso2)
    wb_gov = merge_wb_sticky(new_wb, prev_by_iso2.get(iso2))

    # ── REST Countries metadata ───────────────────────────────────────────────
    print(f"  [{iso2}] REST Countries fetch...")
    meta = fetch_rest_countries(iso2)
    print(f"  [{iso2}] meta: capital={meta.get('capital')}, pop={meta.get('population')}")

    # ── dataAvailability block ────────────────────────────────────────────────
    static_notes = DATA_AVAILABILITY_NOTES.get(iso2, {})
    avail: Dict[str, str] = {}

    if exec_note:
        avail["executive"] = exec_note
    elif static_notes.get("executive"):
        avail["executive"] = static_notes["executive"]

    if wb_gov.get("overallPercentile") is None:
        avail["worldBankGovernance"] = (
            static_notes.get("worldBankGovernance") or
            f"World Bank WGI data unavailable for '{iso2}'.")
    elif static_notes.get("worldBankGovernance"):
        avail["worldBankGovernance"] = static_notes["worldBankGovernance"]

    if static_notes.get("elections"):
        avail["elections"] = static_notes["elections"]

    if meta.get("capital") is None and meta.get("population") is None:
        avail["metadata"] = f"REST Countries API returned no data for '{iso2}'."

    # ── Assemble ──────────────────────────────────────────────────────────────
    exec_leader = hog_name or hos_name
    exec_party  = hog_party if hog_party not in (None, "unknown") else hos_party

    return {
        "country": name,
        "iso2":    iso2,
        "metadata": {
            "officialName": meta["officialName"],
            "capital":      meta["capital"],
            "population":   meta["population"],
            "region":       meta["region"],
            "subregion":    meta["subregion"],
            "flag":         meta["flag"],
            "flagPng":      meta["flagPng"],
            "currencies":   meta["currencies"],
            "languages":    meta["languages"],
            "source":       meta["source"],
        },
        "politicalSystem": {
            "values": pol_sys,
            "source": "static_ground_truth",
        },
        "executive": {
            "headOfState": {
                "name":         hos_name,
                "partyOrGroup": hos_party,
                "source":       exec_source,
            },
            "headOfGovernment": {
                "name":         hog_name,
                "partyOrGroup": hog_party,
                "source":       exec_source,
            },
            "executiveInPower": {
                "leader":       exec_leader,
                "partyOrGroup": exec_party,
                "method":       "head_of_government" if hog_name else "head_of_state",
            },
        },
        "legislature": {
            "bodies": legislature,
            "source": "static_ground_truth",
        },
        "worldBankGovernance": wb_gov,
        "dataAvailability": avail if avail else None,
        "elections": elections,
    }

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("docs") / "countries_snapshot.json"
    prev = load_previous_snapshot(out_path)
    print(f"=== Starting build. Previous snapshot: {len(prev)} countries cached ===")

    # Pre-load shared caches (one HTTP round-trip each, reused for all countries)
    _load_ipu_parliament_map()
    _load_electionguide_cache()
    _load_wiki_exec_cache()

    out = {
        "generatedAt":        iso_z(now_utc()),
        "worldBankYearRule":  "latest_non_null_per_indicator",
        "countries":          [],
        "sources": {
            "executives":            "static_ground_truth (Wikipedia-verified, March 2026)",
            "legislature":           "static_ground_truth (March 2026)",
            "elections":             "static_ground_truth + IPU Parline + ElectionGuide (adaptive enrichment)",
            "wikipedia_adaptive":    WIKIPEDIA_API,
            "world_bank_base":       WORLD_BANK_BASE,
            "ipu_parline":           f"{IPU_API_BASE}/api",
            "electionguide":         ELECTIONGUIDE_BASE,
            "rest_countries":        REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
        "electionDataModel": {
            "description": (
                "Each country's elections block contains competitiveElections (bool), "
                "nonCompetitiveReason (str|null for one-party/non-democratic states), "
                "and legislative/executive sub-blocks each with lastElection and nextElection. "
                "Election objects contain: date (ISO string or year), type (string description), "
                "notes (context string)."
            ),
        },
    }

    for c in COUNTRIES:
        print(f"\n▶ {c['country']} ({c['iso2']})")
        out["countries"].append(build_country(c["country"], c["iso2"], prev))
        time.sleep(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")


if __name__ == "__main__":
    main()
