"""
Build a Base44-friendly JSON snapshot for multiple countries.
Output: public/countries_snapshot.json

Run:  python scripts/build_countries_snapshot.py
Deps: pip install requests

Data strategy (as of March 2026):
  - Executive names/parties:   STATIC_COUNTRY_DATA (ground-truth, Wikipedia-verified)
  - Legislature bodies/control: STATIC_COUNTRY_DATA (ground-truth)
  - Elections:                  STATIC_COUNTRY_DATA + IPU adaptive enrichment
  - Political system:           STATIC_COUNTRY_DATA (cleaned)
  - Metadata:                   REST Countries API (adaptive)
  - Governance:                 World Bank WGI API (adaptive)
  - Wikipedia scrape:           Adaptive enrichment, overrides static names if fresher
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

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

WIKIDATA_SPARQL     = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE     = "https://api.worldbank.org/v2"
IPU_API_BASE        = "https://api.data.ipu.org/v1"
REST_COUNTRIES_BASE = "https://restcountries.com/v3.1"
WIKIPEDIA_API       = "https://en.wikipedia.org/w/api.php"

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

# Taiwan is not in IPU (not a UN member state). None = skip gracefully.
IPU_ISO2_OVERRIDES: Dict[str, Optional[str]] = {
    "TW": None,
    "KP": None,  # North Korea not meaningfully tracked by IPU
}

# ── STATIC GROUND-TRUTH COUNTRY DATA ─────────────────────────────────────────
# Verified as of March 2026. Each entry is the authoritative baseline.
# Adaptive sources (Wikipedia, IPU) can enrich/update these at runtime.
#
# Fields per country:
#   hosName, hosParty       – Head of State name and party/affiliation
#   hogName, hogParty       – Head of Government name and party/affiliation
#   executiveNote           – Optional context note
#   politicalSystem         – List of clean system descriptors
#   legislature             – List of {name, inControl} dicts
#   elections               – {legislative: {lastDate, nextDate}, executive: {nextDate}}
#   dataNote                – Optional note on data limitations

STATIC_COUNTRY_DATA: Dict[str, Dict] = {
    "RU": {
        "hosName":  "Vladimir Putin",
        "hosParty": "United Russia",
        "hogName":  "Mikhail Mishustin",
        "hogParty": "United Russia",
        "politicalSystem": ["presidential republic", "federal state"],
        "legislature": [
            {"name": "State Duma", "inControl": "United Russia"},
            {"name": "Federation Council", "inControl": "United Russia"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-09-19", "nextDate": "2026-09"},
            "executive":   {"lastDate": "2024-03-17", "nextDate": "2030"},
        },
    },
    "IN": {
        "hosName":  "Droupadi Murmu",
        "hosParty": "Bharatiya Janata Party",
        "hogName":  "Narendra Modi",
        "hogParty": "Bharatiya Janata Party",
        "politicalSystem": ["federal parliamentary democratic republic"],
        "legislature": [
            {"name": "Lok Sabha (lower house)", "inControl": "BJP-led National Democratic Alliance"},
            {"name": "Rajya Sabha (upper house)", "inControl": "BJP-led National Democratic Alliance"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-04-19", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-07", "nextDate": "2029"},
        },
    },
    "PK": {
        "hosName":  "Asif Ali Zardari",
        "hosParty": "Pakistan Peoples Party",
        "hogName":  "Shehbaz Sharif",
        "hogParty": "Pakistan Muslim League (N)",
        "politicalSystem": ["federal parliamentary constitutional republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "PML-N-led coalition"},
            {"name": "Senate", "inControl": "Coalition government"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-02-08", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-03", "nextDate": "2029"},
        },
    },
    "CN": {
        "hosName":  "Xi Jinping",
        "hosParty": "Chinese Communist Party",
        "hogName":  "Li Qiang",
        "hogParty": "Chinese Communist Party",
        "executiveNote": "Xi Jinping holds supreme authority as General Secretary of the CCP and CMC Chairman; Li Qiang is Premier (head of government).",
        "politicalSystem": ["one-party socialist state"],
        "legislature": [
            {"name": "National People's Congress", "inControl": "Chinese Communist Party"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-03", "nextDate": "2028"},
            "executive":   {"lastDate": "2023-03", "nextDate": "2028"},
        },
    },
    "GB": {
        "hosName":  "King Charles III",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Keir Starmer",
        "hogParty": "Labour Party",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy", "unitary state"],
        "legislature": [
            {"name": "House of Commons", "inControl": "Labour Party"},
            {"name": "House of Lords", "inControl": "Cross-bench (appointed, non-elected)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-07-04", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-07-04", "nextDate": "2029"},
        },
    },
    "DE": {
        "hosName":  "Frank-Walter Steinmeier",
        "hosParty": "Social Democratic Party",
        "hogName":  "Friedrich Merz",
        "hogParty": "Christian Democratic Union",
        "executiveNote": "CDU/CSU won the Feb 2025 election; Merz became Chancellor in April 2025.",
        "politicalSystem": ["federal parliamentary republic"],
        "legislature": [
            {"name": "Bundestag", "inControl": "CDU/CSU-led coalition"},
            {"name": "Bundesrat", "inControl": "State governments (non-partisan body)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2025-02-23", "nextDate": "2029"},
            "executive":   {"lastDate": "2025-02-23", "nextDate": "2029"},
        },
    },
    "AE": {
        "hosName":  "Mohamed bin Zayed Al Nahyan",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mohammed bin Rashid Al Maktoum",
        "hogParty": "Non-partisan (monarchy)",
        "executiveNote": "UAE is a federal monarchy; no elections for executive positions.",
        "politicalSystem": ["federal constitutional monarchy", "absolute monarchy"],
        "legislature": [
            {"name": "Federal National Council", "inControl": "Non-partisan (advisory body; half appointed, half indirectly elected)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-10-07", "nextDate": "2027"},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "SA": {
        "hosName":  "King Salman bin Abdulaziz Al Saud",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mohammed bin Salman",
        "hogParty": "Non-partisan (monarchy)",
        "executiveNote": "King Salman is head of state; his son Crown Prince Mohammed bin Salman (MBS) serves as Prime Minister and is de facto ruler since 2022.",
        "politicalSystem": ["absolute monarchy", "theocratic state"],
        "legislature": [
            {"name": "Consultative Assembly (Majlis al-Shura)", "inControl": "Royal appointments (no elections)"},
        ],
        "elections": {
            "legislative": {"lastDate": None, "nextDate": None},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "IL": {
        "hosName":  "Isaac Herzog",
        "hosParty": "Non-partisan (ceremonial president)",
        "hogName":  "Benjamin Netanyahu",
        "hogParty": "Likud",
        "politicalSystem": ["parliamentary democracy", "unitary republic"],
        "legislature": [
            {"name": "Knesset", "inControl": "Likud-led right-wing coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2022-11-01", "nextDate": "2026-10-27"},
            "executive":   {"lastDate": "2022-11-01", "nextDate": "2026-10-27"},
        },
    },
    "PS": {
        "hosName":  "Mahmoud Abbas",
        "hosParty": "Fatah",
        "hogName":  "Mohammad Mustafa",
        "hogParty": "Fatah",
        "executiveNote": "Mahmoud Abbas has ruled by decree since Palestinian elections were last held in 2006. PM Mohammad Mustafa appointed March 2024.",
        "politicalSystem": ["semi-presidential republic", "disputed/occupied territory"],
        "legislature": [
            {"name": "Palestinian Legislative Council", "inControl": "Suspended (no elections since 2006)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2006-01-25", "nextDate": None},
            "executive":   {"lastDate": "2005-01-09", "nextDate": None},
        },
    },
    "MX": {
        "hosName":  "Claudia Sheinbaum",
        "hosParty": "Morena",
        "hogName":  "Claudia Sheinbaum",
        "hogParty": "Morena",
        "executiveNote": "Mexico has a presidential system; Sheinbaum is both head of state and head of government. First elected female president of Mexico.",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Morena-led coalition (supermajority)"},
            {"name": "Senate", "inControl": "Morena-led coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-06-02", "nextDate": "2027"},
            "executive":   {"lastDate": "2024-06-02", "nextDate": "2030"},
        },
    },
    "BR": {
        "hosName":  "Luiz Inácio Lula da Silva",
        "hosParty": "Workers' Party (PT)",
        "hogName":  "Luiz Inácio Lula da Silva",
        "hogParty": "Workers' Party (PT)",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Centre-right coalition (PL largest party)"},
            {"name": "Federal Senate", "inControl": "Coalition government (PSD largest bloc)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2022-10-02", "nextDate": "2026-10"},
            "executive":   {"lastDate": "2022-10-30", "nextDate": "2026-10"},
        },
    },
    "CA": {
        "hosName":  "King Charles III (rep. Governor General Mary Simon)",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mark Carney",
        "hogParty": "Liberal Party",
        "executiveNote": "Mark Carney became PM on 14 March 2025 after Trudeau's resignation. Liberals won the 28 April 2025 election with 169 seats — a minority government, 3 seats short of majority. Several opposition MPs have since crossed the floor.",
        "politicalSystem": ["federal parliamentary constitutional monarchy"],
        "legislature": [
            {"name": "House of Commons", "inControl": "Liberal Party (minority government, 169/343 seats)"},
            {"name": "Senate", "inControl": "Non-partisan (Independent Senators Group largest bloc)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2025-04-28", "nextDate": "2029"},
            "executive":   {"lastDate": "2025-04-28", "nextDate": "2029"},
        },
    },
    "NG": {
        "hosName":  "Bola Tinubu",
        "hosParty": "All Progressives Congress",
        "hogName":  "Bola Tinubu",
        "hogParty": "All Progressives Congress",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "All Progressives Congress"},
            {"name": "Senate", "inControl": "All Progressives Congress"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-02-25", "nextDate": "2027"},
            "executive":   {"lastDate": "2023-02-25", "nextDate": "2027"},
        },
    },
    "JP": {
        "hosName":  "Emperor Naruhito",
        "hosParty": "Non-partisan (imperial household)",
        "hogName":  "Sanae Takaichi",
        "hogParty": "Liberal Democratic Party",
        "executiveNote": "Sanae Takaichi became Japan's first female PM in October 2025. She called a snap election on 8 February 2026, winning a historic LDP supermajority (316/465 seats in the lower house).",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "House of Representatives (Shugiin)", "inControl": "LDP (supermajority — 316/465 seats, Feb 2026)"},
            {"name": "House of Councillors (Sangiin)", "inControl": "LDP-led coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2026-02-08", "nextDate": "2030"},
            "executive":   {"lastDate": "2026-02-08", "nextDate": "2030"},
        },
    },
    "IR": {
        "hosName":  "Interim Leadership Council",
        "hosParty": "Islamic Republic (transitional)",
        "hogName":  "Masoud Pezeshkian",
        "hogParty": "Reformist front",
        "executiveNote": "Ali Khamenei was killed on 28 Feb 2026 in US/Israeli strikes. A three-member interim council (President, Chief Justice, Guardian Council rep) is overseeing transition to elect a new Supreme Leader. Masoud Pezeshkian remains President.",
        "politicalSystem": ["theocratic republic", "Islamic republic (transitional)"],
        "legislature": [
            {"name": "Islamic Consultative Assembly (Majlis)", "inControl": "Conservative/Principlist majority"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-03-01", "nextDate": "2028"},
            "executive":   {"lastDate": "2024-07-05", "nextDate": "2028"},
        },
    },
    "SY": {
        "hosName":  "Ahmad al-Sharaa",
        "hosParty": "Hayat Tahrir al-Sham (transitional authority)",
        "hogName":  "Mohammad al-Bashir",
        "hogParty": "Transitional government",
        "executiveNote": "Assad fled in December 2024. Ahmad al-Sharaa (formerly Abu Mohammad al-Jolani) leads the transitional government. No functioning legislature.",
        "politicalSystem": ["transitional government (post-civil war)"],
        "legislature": [
            {"name": "No functioning legislature", "inControl": "Transitional authority"},
        ],
        "elections": {
            "legislative": {"lastDate": None, "nextDate": None},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "FR": {
        "hosName":  "Emmanuel Macron",
        "hosParty": "Renaissance",
        "hogName":  "François Bayrou",
        "hogParty": "Democratic Movement (MoDem)",
        "executiveNote": "François Bayrou became PM in January 2025 after Michel Barnier's government fell in December 2024.",
        "politicalSystem": ["unitary semi-presidential republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "No single majority (hung parliament; left-wing NFP largest bloc)"},
            {"name": "Senate", "inControl": "Centre-right (Les Républicains largest group)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-07-07", "nextDate": "2029"},
            "executive":   {"lastDate": "2022-04-24", "nextDate": "2027-04"},
        },
    },
    "TR": {
        "hosName":  "Recep Tayyip Erdoğan",
        "hosParty": "Justice and Development Party (AKP)",
        "hogName":  "Recep Tayyip Erdoğan",
        "hogParty": "Justice and Development Party (AKP)",
        "executiveNote": "Turkey has a presidential system; Erdoğan is both head of state and government.",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "Grand National Assembly", "inControl": "AKP-led People's Alliance"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-05-14", "nextDate": "2028"},
            "executive":   {"lastDate": "2023-05-28", "nextDate": "2028"},
        },
    },
    "VE": {
        "hosName":  "Delcy Rodríguez (acting)",
        "hosParty": "United Socialist Party of Venezuela (PSUV)",
        "hogName":  "Delcy Rodríguez (acting)",
        "hogParty": "United Socialist Party of Venezuela (PSUV)",
        "executiveNote": "Nicolás Maduro fled Venezuela in late 2025; Vice President Delcy Rodríguez became acting president on 3 January 2026. Edmundo González is internationally recognised by some states as legitimate president-elect.",
        "politicalSystem": ["presidential republic (disputed/authoritarian)"],
        "legislature": [
            {"name": "National Assembly", "inControl": "PSUV (government-aligned majority)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2020-12-06", "nextDate": None},
            "executive":   {"lastDate": "2024-07-28", "nextDate": "2030"},
        },
    },
    "VN": {
        "hosName":  "Lương Cường",
        "hosParty": "Communist Party of Vietnam",
        "hogName":  "Phạm Minh Chính",
        "hogParty": "Communist Party of Vietnam",
        "executiveNote": "Lương Cường became President in October 2024. The General Secretary (Tô Lâm) holds supreme authority as party leader.",
        "politicalSystem": ["one-party socialist republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Communist Party of Vietnam"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-05-23", "nextDate": "2026-05"},
            "executive":   {"lastDate": "2021-07", "nextDate": "2026"},
        },
    },
    "TW": {
        "hosName":  "Lai Ching-te",
        "hosParty": "Democratic Progressive Party (DPP)",
        "hogName":  "Cho Jung-tai",
        "hogParty": "Democratic Progressive Party (DPP)",
        "executiveNote": "Taiwan is not a UN member state; its sovereignty is disputed. Lai Ching-te (William Lai) became president in May 2024.",
        "politicalSystem": ["semi-presidential republic (disputed sovereignty)"],
        "legislature": [
            {"name": "Legislative Yuan", "inControl": "Kuomintang (KMT) and Taiwan People's Party (TPP) hold combined majority"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-01-13", "nextDate": "2028"},
            "executive":   {"lastDate": "2024-01-13", "nextDate": "2028"},
        },
    },
    "KR": {
        "hosName":  "Lee Jae-myung",
        "hosParty": "Democratic Party of Korea",
        "hogName":  "Lee Jae-myung",
        "hogParty": "Democratic Party of Korea",
        "executiveNote": "Lee Jae-myung won the June 2025 snap presidential election following Yoon Suk-yeol's impeachment and removal for his December 2024 martial law attempt.",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Democratic Party of Korea (supermajority)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-04-10", "nextDate": "2028"},
            "executive":   {"lastDate": "2025-06-03", "nextDate": "2030"},
        },
    },
    "KP": {
        "hosName":  "Kim Jong-un",
        "hosParty": "Korean Workers' Party",
        "hogName":  "Kim Jong-un",
        "hogParty": "Korean Workers' Party",
        "executiveNote": "Kim Jong-un holds supreme authority as General Secretary of the KWP, President of State Affairs, and Supreme Commander.",
        "politicalSystem": ["one-party totalitarian state", "hereditary dictatorship"],
        "legislature": [
            {"name": "Supreme People's Assembly", "inControl": "Korean Workers' Party (single-party)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-01-07", "nextDate": "2029"},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "ID": {
        "hosName":  "Prabowo Subianto",
        "hosParty": "Gerindra Party",
        "hogName":  "Prabowo Subianto",
        "hogParty": "Gerindra Party",
        "executiveNote": "Prabowo Subianto became president in October 2024, succeeding Joko Widodo.",
        "politicalSystem": ["presidential republic", "unitary state"],
        "legislature": [
            {"name": "People's Representative Council (DPR)", "inControl": "Prabowo-allied coalition (majority)"},
            {"name": "Regional Representative Council (DPD)", "inControl": "Non-partisan (regional representatives)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-02-14", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-02-14", "nextDate": "2029"},
        },
    },
    "MM": {
        "hosName":  "Min Aung Hlaing",
        "hosParty": "Tatmadaw (military)",
        "hogName":  "Min Aung Hlaing",
        "hogParty": "Tatmadaw (military)",
        "executiveNote": "Myanmar has been under military junta (SAC) rule since the February 2021 coup. The elected NLD government operates in exile as the National Unity Government.",
        "politicalSystem": ["military junta (State Administration Council)"],
        "legislature": [
            {"name": "Pyidaungsu Hluttaw (suspended)", "inControl": "Dissolved by military coup (Feb 2021)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2020-11-08", "nextDate": None},
            "executive":   {"lastDate": "2020-11-08", "nextDate": None},
        },
    },
    "AM": {
        "hosName":  "Vahagn Khachaturyan",
        "hosParty": "Non-partisan (ceremonial)",
        "hogName":  "Nikol Pashinyan",
        "hogParty": "Civil Contract",
        "politicalSystem": ["parliamentary republic", "unitary state"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Civil Contract (majority)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-06-20", "nextDate": "2026"},
            "executive":   {"lastDate": "2022-03-03", "nextDate": "2027"},
        },
    },
    "AZ": {
        "hosName":  "Ilham Aliyev",
        "hosParty": "New Azerbaijan Party",
        "hogName":  "Ali Asadov",
        "hogParty": "New Azerbaijan Party",
        "politicalSystem": ["presidential republic (authoritarian)"],
        "legislature": [
            {"name": "National Assembly (Milli Majlis)", "inControl": "New Azerbaijan Party"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-09-01", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-02-07", "nextDate": "2031"},
        },
    },
    "MA": {
        "hosName":  "King Mohammed VI",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Aziz Akhannouch",
        "hogParty": "National Rally of Independents (RNI)",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "RNI-led coalition"},
            {"name": "House of Councillors", "inControl": "RNI-led coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-09-08", "nextDate": "2026"},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "SO": {
        "hosName":  "Hassan Sheikh Mohamud",
        "hosParty": "Union for Peace and Development",
        "hogName":  "Hamza Abdi Barre",
        "hogParty": "Union for Peace and Development",
        "executiveNote": "Somalia has a fragile federal government; direct elections are limited. Hassan Sheikh Mohamud was re-elected by parliament in May 2022.",
        "politicalSystem": ["federal parliamentary republic (fragile state)"],
        "legislature": [
            {"name": "People's Assembly (Lower House)", "inControl": "Union for Peace and Development (plurality)"},
            {"name": "Upper House (Senate)", "inControl": "Coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-10", "nextDate": "2025-2026"},
            "executive":   {"lastDate": "2022-05-15", "nextDate": "2026"},
        },
    },
    "YE": {
        "hosName":  "Rashad al-Alimi (Presidential Leadership Council chair)",
        "hosParty": "Coalition (Presidential Leadership Council)",
        "hogName":  "Ahmed Awad bin Mubarak",
        "hogParty": "Internationally recognised government",
        "executiveNote": "Yemen is split between the Houthi-controlled north (Ansar Allah) and the internationally recognised government in the south. The Presidential Leadership Council (est. 2022) chairs the recognised government.",
        "politicalSystem": ["republic (de facto divided/civil war)"],
        "legislature": [
            {"name": "House of Representatives (suspended)", "inControl": "Divided (parallel governments)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2003-04-27", "nextDate": None},
            "executive":   {"lastDate": "2012-02-21", "nextDate": None},
        },
    },
    "LY": {
        "hosName":  "Mohamed al-Menfi (GNU Presidential Council)",
        "hosParty": "Non-partisan (UN-backed)",
        "hogName":  "Abdul Hamid Dbeibeh (GNU)",
        "hogParty": "Non-partisan",
        "executiveNote": "Libya has two rival governments: the UN-recognised Government of National Unity (GNU) in Tripoli, and the rival government backed by the House of Representatives in Benghazi/Tobruk.",
        "politicalSystem": ["transitional republic (divided/rival governments)"],
        "legislature": [
            {"name": "House of Representatives (HoR)", "inControl": "Eastern-based rival government"},
            {"name": "Government of National Unity (Tripoli)", "inControl": "UN-recognised"},
        ],
        "elections": {
            "legislative": {"lastDate": "2014-06-25", "nextDate": None},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "EG": {
        "hosName":  "Abdel Fattah el-Sisi",
        "hosParty": "No party (military-backed)",
        "hogName":  "Mostafa Madbouly",
        "hogParty": "No party (technocratic)",
        "politicalSystem": ["presidential republic (authoritarian)"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "Pro-Sisi independents and Nation's Future Party"},
            {"name": "Senate", "inControl": "Pro-Sisi majority"},
        ],
        "elections": {
            "legislative": {"lastDate": "2020-10-24", "nextDate": "2025"},
            "executive":   {"lastDate": "2023-12-10", "nextDate": "2030"},
        },
    },
    "DZ": {
        "hosName":  "Abdelmadjid Tebboune",
        "hosParty": "National Liberation Front (FLN) aligned",
        "hogName":  "Nadir Larbaoui",
        "hogParty": "National Liberation Front (FLN) aligned",
        "politicalSystem": ["presidential republic (dominant-party)"],
        "legislature": [
            {"name": "People's National Assembly", "inControl": "FLN-led coalition"},
            {"name": "Council of the Nation", "inControl": "FLN-led coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-06-12", "nextDate": "2027"},
            "executive":   {"lastDate": "2024-09-07", "nextDate": "2029"},
        },
    },
    "AR": {
        "hosName":  "Javier Milei",
        "hosParty": "La Libertad Avanza",
        "hogName":  "Javier Milei",
        "hogParty": "La Libertad Avanza",
        "politicalSystem": ["federal presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "No single majority (Milei is minority; Peronist blocs largest)"},
            {"name": "Senate", "inControl": "No single majority"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-10-22", "nextDate": "2025-10"},
            "executive":   {"lastDate": "2023-11-19", "nextDate": "2027"},
        },
    },
    "CL": {
        "hosName":  "Gabriel Boric",
        "hosParty": "Apruebo Dignidad (Broad Front / PC coalition)",
        "hogName":  "Gabriel Boric",
        "hogParty": "Apruebo Dignidad",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "Chamber of Deputies", "inControl": "Right-wing Chile Vamos coalition (largest bloc)"},
            {"name": "Senate", "inControl": "Right-wing coalition (majority)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-11-21", "nextDate": "2025-11"},
            "executive":   {"lastDate": "2021-12-19", "nextDate": "2025-11"},
        },
    },
    "PE": {
        "hosName":  "Dina Boluarte",
        "hosParty": "Free Peru (Perú Libre) – estranged",
        "hogName":  "Dina Boluarte",
        "hogParty": "No active party affiliation",
        "executiveNote": "Dina Boluarte became president in December 2022 after Pedro Castillo's impeachment. She has governed without a stable congressional majority.",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "Congress", "inControl": "Right-wing and centre-right coalition (Alliance for Progress largest bloc)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2021-04-11", "nextDate": "2026-04"},
            "executive":   {"lastDate": "2021-06-06", "nextDate": "2026-04"},
        },
    },
    "CU": {
        "hosName":  "Miguel Díaz-Canel",
        "hosParty": "Communist Party of Cuba",
        "hogName":  "Manuel Marrero Cruz",
        "hogParty": "Communist Party of Cuba",
        "politicalSystem": ["one-party socialist republic"],
        "legislature": [
            {"name": "National Assembly of People's Power", "inControl": "Communist Party of Cuba"},
        ],
        "elections": {
            "legislative": {"lastDate": "2023-03-26", "nextDate": "2028"},
            "executive":   {"lastDate": "2023-04", "nextDate": "2028"},
        },
    },
    "CO": {
        "hosName":  "Gustavo Petro",
        "hosParty": "Colombia Humana / Pacto Histórico",
        "hogName":  "Gustavo Petro",
        "hogParty": "Pacto Histórico",
        "politicalSystem": ["unitary presidential republic"],
        "legislature": [
            {"name": "House of Representatives", "inControl": "Fragmented (Pacto Histórico minority; no clear majority)"},
            {"name": "Senate", "inControl": "Fragmented coalition"},
        ],
        "elections": {
            "legislative": {"lastDate": "2022-03-13", "nextDate": "2026-03"},
            "executive":   {"lastDate": "2022-06-19", "nextDate": "2026-05"},
        },
    },
    "PA": {
        "hosName":  "José Raúl Mulino",
        "hosParty": "Realizing Goals Party",
        "hogName":  "José Raúl Mulino",
        "hogParty": "Realizing Goals Party",
        "politicalSystem": ["presidential republic"],
        "legislature": [
            {"name": "National Assembly", "inControl": "Fragmented (no single majority; Realizing Goals is largest party)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-05-05", "nextDate": "2029"},
            "executive":   {"lastDate": "2024-05-05", "nextDate": "2029"},
        },
    },
    "SV": {
        "hosName":  "Nayib Bukele",
        "hosParty": "New Ideas",
        "hogName":  "Nayib Bukele",
        "hogParty": "New Ideas",
        "executiveNote": "Nayib Bukele was re-elected in February 2024 despite constitutional single-term limits; the Supreme Court approved his candidacy.",
        "politicalSystem": ["presidential republic"],
        "legislature": [
            {"name": "Legislative Assembly", "inControl": "New Ideas (supermajority)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2024-02-04", "nextDate": "2027"},
            "executive":   {"lastDate": "2024-02-04", "nextDate": "2030"},
        },
    },
    "DK": {
        "hosName":  "King Frederik X",
        "hosParty": "Non-partisan (monarchy)",
        "hogName":  "Mette Frederiksen",
        "hogParty": "Social Democrats",
        "executiveNote": "King Frederik X succeeded Queen Margrethe II on 14 January 2024. PM Frederiksen called a snap election for 24 March 2026, capitalising on popularity from her stance against Trump's Greenland threats.",
        "politicalSystem": ["constitutional monarchy", "parliamentary democracy"],
        "legislature": [
            {"name": "Folketing", "inControl": "Social Democrat-led centre-left coalition (election 24 Mar 2026 pending)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2022-11-01", "nextDate": "2026-03-24"},
            "executive":   {"lastDate": "2022-11-01", "nextDate": "2026-03-24"},
        },
    },
    "SD": {
        "hosName":  "Abdel Fattah al-Burhan",
        "hosParty": "Sudanese Armed Forces (SAF)",
        "hogName":  "Abdel Fattah al-Burhan",
        "hogParty": "Sudanese Armed Forces (SAF)",
        "executiveNote": "Sudan has been in civil war since April 2023 between the SAF (al-Burhan) and RSF (Hemeti). The civilian transitional framework has collapsed.",
        "politicalSystem": ["military junta (transitional sovereignty council)"],
        "legislature": [
            {"name": "No functioning legislature", "inControl": "Dissolved; civil war ongoing"},
        ],
        "elections": {
            "legislative": {"lastDate": None, "nextDate": None},
            "executive":   {"lastDate": None, "nextDate": None},
        },
    },
    "UA": {
        "hosName":  "Volodymyr Zelensky",
        "hosParty": "Servant of the People",
        "hogName":  "Denys Shmyhal",
        "hogParty": "Servant of the People",
        "executiveNote": "Elections are suspended under martial law due to the ongoing Russian invasion. Zelensky's term was extended per wartime provisions.",
        "politicalSystem": ["semi-presidential republic (martial law)"],
        "legislature": [
            {"name": "Verkhovna Rada", "inControl": "Servant of the People (majority; elections suspended)"},
        ],
        "elections": {
            "legislative": {"lastDate": "2019-07-21", "nextDate": None},
            "executive":   {"lastDate": "2019-04-21", "nextDate": None},
        },
    },
}

# ── DATA AVAILABILITY NOTES ───────────────────────────────────────────────────

DATA_AVAILABILITY_NOTES: Dict[str, Dict[str, str]] = {
    "TW": {
        "worldBankGovernance": (
            "Taiwan is not a UN member state and is not recognised by the World Bank. "
            "WGI data is unavailable."
        ),
        "elections.legislative": (
            "Taiwan is not a member of the IPU and is absent from the Parline database."
        ),
    },
    "PS": {
        "worldBankGovernance": (
            "World Bank data for Palestine is limited due to its political status."
        ),
    },
    "KP": {
        "worldBankGovernance": (
            "North Korea governance data is based on limited external assessments."
        ),
        "elections.legislative": (
            "North Korea holds nominal single-party elections; meaningful data unavailable."
        ),
    },
    "SY": {
        "executive": (
            "Syria's transitional government (post-Assad, Dec 2024) has no formal electoral basis. "
            "Data is from static records."
        ),
    },
    "SO": {
        "worldBankGovernance": (
            "Somalia governance data is based on limited external assessments."
        ),
    },
    "YE": {
        "worldBankGovernance": (
            "Yemen governance data reflects the internationally recognised government baseline; "
            "current effective governance is severely disrupted by civil war."
        ),
    },
    "LY": {
        "worldBankGovernance": (
            "Libya has parallel governing authorities; data reflects the "
            "internationally recognised Government of National Unity's institutional capacity."
        ),
    },
    "SD": {
        "worldBankGovernance": (
            "Sudan is in active civil war (SAF vs RSF since April 2023); governance data "
            "is from pre-war assessments and does not reflect current conditions."
        ),
    },
    "UA": {
        "elections.legislative": (
            "Elections suspended under martial law due to Russian invasion (since Feb 2022)."
        ),
    },
    "IR": {
        "executive": (
            "Ali Khamenei was killed on 28 Feb 2026. An interim leadership council "
            "is in place pending election of a new Supreme Leader."
        ),
    },
    "VE": {
        "executive": (
            "The July 2024 presidential election is disputed internationally. "
            "Delcy Rodríguez serves as acting president; Edmundo González is recognised "
            "by some states as the legitimate president-elect."
        ),
    },
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    label: str = "",
) -> Optional[Any]:
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
# Best-effort: enriches static data when Wikipedia has fresher names.
# Static data always wins if Wikipedia parse fails.

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
        print("  [WIKI] Failed to fetch Wikipedia list — using static data only")
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
            if tag == "table":
                cls = attrs_dict.get("class", "")
                if "wikitable" in cls:
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
    # Build reverse map: wikipedia name → iso2
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
            # fuzzy: check if any known name is substring
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

# ── WIKIDATA (structural data only) ──────────────────────────────────────────

def wikidata_sparql(query: str) -> Optional[dict]:
    return req_json(
        WIKIDATA_SPARQL,
        params={"format": "json", "query": query},
        headers={"Accept": "application/sparql-results+json"},
        label="Wikidata SPARQL",
    )

def _wd(b: dict, key: str) -> Optional[str]:
    v = b.get(key)
    return v.get("value") if v else None

def get_qid(iso2: str) -> Optional[str]:
    data = wikidata_sparql(f'SELECT ?c WHERE {{ ?c wdt:P297 "{iso2}" . }} LIMIT 1')
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return None
    uri = _wd(bindings[0], "c")
    return uri.rsplit("/", 1)[-1] if uri else None

# ── IPU PARLINE (adaptive election dates) ────────────────────────────────────

_ipu_cache: Optional[Dict[str, List[Dict]]] = None

def _load_ipu_cache() -> Dict[str, List[Dict]]:
    global _ipu_cache
    if _ipu_cache is not None:
        return _ipu_cache

    print("  [IPU] Pre-fetching chamber data for all countries...")
    cache: Dict[str, List[Dict]] = {}
    _first_logged = False

    for c in COUNTRIES:
        iso2 = c["iso2"]
        if iso2 in IPU_ISO2_OVERRIDES and IPU_ISO2_OVERRIDES[iso2] is None:
            print(f"  [IPU] Skipping {iso2} (not in IPU)")
            continue

        url = f"{IPU_API_BASE}/chambers/{iso2.upper()}"
        data = req_json(url, label=f"IPU /chambers/{iso2}")
        if not data:
            print(f"  [IPU] {iso2}: no data returned")
            continue

        if not _first_logged:
            if isinstance(data, dict):
                top = list(data.keys())[:10]
                inner = data.get("data")
                print(f"  [IPU] FIRST RESPONSE SHAPE ({iso2}): dict, top_keys={top}, "
                      f"data_type={type(inner).__name__}")
                if isinstance(inner, list) and inner:
                    print(f"  [IPU]   data[0] keys: {list(inner[0].keys())[:12]}")
            elif isinstance(data, list):
                print(f"  [IPU] FIRST RESPONSE SHAPE ({iso2}): list[{len(data)}]")
                if data and isinstance(data[0], dict):
                    print(f"  [IPU]   [0] keys: {list(data[0].keys())[:12]}")
            _first_logged = True

        chambers: List[Dict] = []
        if isinstance(data, list):
            chambers = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            raw = data.get("data")
            if isinstance(raw, list):
                chambers = [r for r in raw if isinstance(r, dict)]
            elif isinstance(raw, dict):
                chambers = [raw]
            elif raw is None:
                if any(k in data for k in ("last_election_date",
                                            "expect_date_next_election",
                                            "country_code")):
                    chambers = [data]

        unwrapped: List[Dict] = []
        for ch in chambers:
            attrs = ch.get("attributes")
            unwrapped.append(attrs if isinstance(attrs, dict) else ch)

        if unwrapped:
            first = unwrapped[0]
            date_fields = {k: v for k, v in first.items()
                           if "date" in k.lower() or "election" in k.lower()}
            print(f"  [IPU] {iso2}: {len(unwrapped)} chamber(s), date/election fields={date_fields}")
            cache[iso2.upper()] = unwrapped
        else:
            print(f"  [IPU] {iso2}: parsed to 0 chambers")

        time.sleep(0.15)

    print(f"  [IPU] Cache complete: {len(cache)} countries with data")
    _ipu_cache = cache
    return cache

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

def fetch_ipu_leg_election(iso2: str) -> Dict[str, Any]:
    if iso2 in IPU_ISO2_OVERRIDES and IPU_ISO2_OVERRIDES[iso2] is None:
        return {"lastDate": None, "nextDate": None, "chamberType": None,
                "notes": f"{iso2} not in IPU Parline."}

    chambers = _load_ipu_cache().get(iso2.upper(), [])
    if not chambers:
        return {"lastDate": None, "nextDate": None, "chamberType": None,
                "notes": f"No IPU data for {iso2}."}

    def _priority(ch: dict) -> int:
        s = str(ch.get("struct_parl_status") or "").lower()
        if "lower" in s or "unicameral" in s: return 0
        if "upper" in s: return 1
        return 2

    best = sorted(chambers, key=_priority)[0]
    return {
        "lastDate":    _parse_ipu_date(best.get("last_election_date")),
        "nextDate":    _parse_ipu_date(best.get("expect_date_next_election")),
        "chamberType": best.get("struct_parl_status"),
        "notes":       "From IPU Parline parliamentary election schedule.",
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
        kept["notes"] = (f"Kept previous values; latest fetch failed: "
                         f"{new_wb.get('notes')}")
        return kept
    out = dict(new_wb)
    out.pop("ok", None)
    return out

# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def build_country(name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
    static = STATIC_COUNTRY_DATA.get(iso2, {})
    if not static:
        print(f"  [{iso2}] WARNING: no static data — output will be sparse")

    # ── Executive: static baseline, optionally enriched by Wikipedia ─────────
    print(f"  [{iso2}] Wikipedia executive lookup...")
    wiki = _load_wiki_exec_cache().get(iso2, {})

    # Static wins for countries with known tricky situations; Wikipedia enriches others
    STATIC_WINS = {"IR", "SY", "VE", "KR", "TW", "MM", "KP", "SD", "YE", "LY"}

    if iso2 in STATIC_WINS or not wiki.get("hosName"):
        hos_name = static.get("hosName")
        hog_name = static.get("hogName")
        exec_source = "static_ground_truth"
    else:
        hos_name = wiki.get("hosName") or static.get("hosName")
        hog_name = wiki.get("hogName") or static.get("hogName")
        exec_source = "wikipedia:List_of_current_heads_of_state_and_government"

    hos_party = static.get("hosParty", "unknown")
    hog_party = static.get("hogParty", "unknown")
    exec_note = static.get("executiveNote")

    print(f"  [{iso2}] HOS={hos_name}, HOG={hog_name}")

    # ── Political system ──────────────────────────────────────────────────────
    pol_sys = static.get("politicalSystem", ["unknown"])

    # ── Legislature (static bodies + control) ────────────────────────────────
    leg_static = static.get("legislature", [{"name": "Legislature", "inControl": "unknown"}])
    legislature = [
        {
            "name":          b["name"],
            "inControl":     b.get("inControl", "unknown"),
            "controlMethod": "static_ground_truth",
            "controlNotes":  None,
            "controlBasis":  None,
        }
        for b in leg_static
    ]

    # ── Elections: static baseline enriched by IPU ───────────────────────────
    print(f"  [{iso2}] IPU fetch...")
    ipu = fetch_ipu_leg_election(iso2)
    print(f"  [{iso2}] IPU: lastDate={ipu.get('lastDate')}, nextDate={ipu.get('nextDate')}")

    static_elec = static.get("elections") or {}
    static_leg  = static_elec.get("legislative") or {}
    static_exec = static_elec.get("executive") or {}

    # IPU wins for lastDate/nextDate if it has data (it's the authoritative election schedule)
    leg_last = ipu.get("lastDate") or static_leg.get("lastDate")
    leg_next = ipu.get("nextDate") or static_leg.get("nextDate")
    leg_source = "IPU Parline" if ipu.get("lastDate") else "static_ground_truth"

    if not ipu.get("lastDate") and static_leg.get("lastDate"):
        print(f"  [{iso2}] WARNING: IPU returned no lastDate - using stale static: {static_leg.get('lastDate')}")
    if not ipu.get("nextDate") and static_leg.get ("nextDate"):
        print(f"  [{iso2}] WARNING: IPU returned no nextDate - using stale static: {static_leg.get('nextDate')}")

    leg_elec = {
        "exists":       True if (leg_last or leg_next) else False,
        "lastDate":     leg_last,
        "nextDate":     leg_next,
        "electionType": ipu.get("chamberType") or "general election",
        "method":       leg_source,
        "notes":        ipu.get("notes") or "From static ground-truth records.",
        "source":       leg_source,
    }

    exec_elec = {
        "exists":       True if static_exec.get("nextDate") else (
                        "unknown" if static_exec.get("lastDate") else False),
        "lastDate":     static_exec.get("lastDate"),
        "nextDate":     static_exec.get("nextDate"),
        "electionType": "presidential election" if static.get("politicalSystem") and
                        any("presidential" in p.lower() for p in static.get("politicalSystem", []))
                        else "indirect/parliamentary selection",
        "method":       "static_ground_truth",
        "notes":        None if static_exec.get("nextDate") else "No executive election scheduled or date unknown.",
        "source":       "static_ground_truth",
    }

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

    if not leg_elec.get("nextDate") and not leg_elec.get("lastDate"):
        avail["elections.legislative"] = (
            static_notes.get("elections.legislative") or
            f"No legislative election data available for '{iso2}'.")
    elif static_notes.get("elections.legislative"):
        avail["elections.legislative"] = static_notes["elections.legislative"]

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
        "elections": {
            "legislative": leg_elec,
            "executive":   exec_elec,
        },
    }

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("public") / "countries_snapshot.json"
    prev = load_previous_snapshot(out_path)
    print(f"=== Starting build. Previous snapshot: {len(prev)} countries cached ===")

    _load_ipu_cache()
    _load_wiki_exec_cache()

    out = {
        "generatedAt":        iso_z(now_utc()),
        "worldBankYearRule":  "latest_non_null_per_indicator",
        "countries":          [],
        "sources": {
            "executives":         "static_ground_truth (Wikipedia-verified, March 2026)",
            "legislature":        "static_ground_truth (March 2026)",
            "elections":          "static_ground_truth + IPU Parline adaptive enrichment",
            "wikipedia_adaptive": WIKIPEDIA_API,
            "world_bank_base":    WORLD_BANK_BASE,
            "ipu_parline":        IPU_API_BASE,
            "rest_countries":     REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
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
