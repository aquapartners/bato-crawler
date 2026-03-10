#!/usr/bin/env python3
"""
Autonomous Global Bonus Crawler
--------------------------------
- Discovers bonus pages from seed URLs via recursive crawling.
- Follows external links to find new domains with incentive content.
- Persists discovered domains for use in future runs (self‑expanding seed list).
- Respects robots.txt, rate limits, and domain scoping.
- Extracts bonus information using custom parsers, CSS selectors, or heuristic fallback.
- Outputs a unified JSON file using your existing transformation logic.
"""

import asyncio
import json
import re
import time
import random
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Set, List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import aiohttp
from playwright.async_api import async_playwright
import robotexclusionrulesparser

# ======================== CRAWL4AI IMPORTS ========================
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

# ======================== CONFIGURATION ========================
DELAY = 2
MAX_PAGES_PER_DOMAIN = 100
MAX_EXTERNAL_LINKS_PER_PAGE = 5
MAX_NEW_DOMAINS_PER_RUN = 50
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

STATIC_SEED_URLS = [
    "https://www.chase.com",
    "https://www.bankofamerica.com",
    "https://www.wellsfargo.com",
    "https://www.citi.com",
    "https://www.capitalone.com",
    "https://www.americanexpress.com",
    "https://www.uber.com",
    "https://www.airbnb.com",
    "https://www.delta.com",
    "https://www.marriott.com",
    "https://www.doctorofcredit.com",
    "https://www.mexc.com",
    "https://www.htx.com",
    "https://crypto.com",
    "https://www.bybit.com",
    "https://www.zillow.com",
    "https://www.redfin.com",
    "https://www.realtor.com",
]

INCENTIVE_KEYWORDS = ["bonus", "referral", "incentive", "promotion", "reward", "cashback", "sign-up", "offer"]
DISCOVERED_DOMAINS_FILE = "discovered_domains.json"

# ======================== ROBOTS.TXT CACHE ========================
robots_parsers: Dict[str, robotexclusionrulesparser.RobotExclusionRulesParser] = {}

async def can_fetch(url: str, user_agent: str = USER_AGENT) -> bool:
    parsed = urlparse(url)
    domain = f"{parsed.scheme}://{parsed.netloc}"
    if domain not in robots_parsers:
        rp = robotexclusionrulesparser.RobotExclusionRulesParser()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{domain}/robots.txt", timeout=5) as resp:
                    if resp.status == 200:
                        rp.parse(await resp.text())
                    else:
                        rp.parse("")
        except Exception:
            rp.parse("")
        robots_parsers[domain] = rp
    return robots_parsers[domain].is_allowed(user_agent, url)

# ======================== PERSISTENT SEEDS ========================
def load_all_seeds() -> List[str]:
    seeds = set(STATIC_SEED_URLS)
    if os.path.exists(DISCOVERED_DOMAINS_FILE):
        try:
            with open(DISCOVERED_DOMAINS_FILE, 'r') as f:
                discovered = json.load(f)
                for item in discovered:
                    if item.startswith('http'):
                        seeds.add(item)
                    else:
                        seeds.add(f"https://{item}")
        except Exception as e:
            print(f"⚠️ Could not load discovered domains: {e}")
    return list(seeds)

def save_new_domains(new_domains: Set[str]) -> None:
    existing = set()
    if os.path.exists(DISCOVERED_DOMAINS_FILE):
        try:
            with open(DISCOVERED_DOMAINS_FILE, 'r') as f:
                existing = set(json.load(f))
        except:
            pass
    all_domains = existing.union(new_domains)
    with open(DISCOVERED_DOMAINS_FILE, 'w') as f:
        json.dump(list(all_domains), f, indent=2)

# ======================== FETCH HELPERS ========================
async def fetch_url_async(url: str) -> Optional[str]:
    if not url or not url.startswith('http'):
        print(f"⚠️ Invalid URL: {url}")
        return None
    if not await can_fetch(url):
        print(f"⚠️ robots.txt disallows {url}")
        return None
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    print(f"HTTP {resp.status} for {url}")
                    return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def fetch_dynamic_async(url: str) -> Optional[str]:
    if not url or not url.startswith('http'):
        print(f"⚠️ Invalid URL: {url}")
        return None
    if not await can_fetch(url):
        print(f"⚠️ robots.txt disallows {url}")
        return None
    browser_config = BrowserConfig(verbose=False, headless=True)
    run_config = CrawlerRunConfig(
        word_count_threshold=10,
        process_iframes=True,
        cache_mode=CacheMode.DISABLED
    )
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)
        if result.success:
            return result.html
        else:
            print(f"crawl4ai error for {url}: {result.error_message}")
            return None

# ======================== EXTRACTION HELPERS ========================
def extract_amount(text):
    match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text)
    if match:
        num_str = match.group(1).replace(',', '')
        try:
            if '.' in num_str:
                return float(num_str)
            else:
                return int(num_str)
        except ValueError:
            return None
    return None

def extract_amount_multi_currency(text):
    match = re.search(r'[£€¥](\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text)
    if match:
        return float(match.group(1).replace(',', ''))
    return extract_amount(text)

def extract_requirements(text):
    req = {
        "min_deposit": None,
        "direct_deposit": None,
        "holding_days": None,
        "transaction_count": None,
        "min_balance": None,
        "geographic_restrictions": [],
        "expiration": None,
        "notes": []
    }
    match = re.search(r'(?:deposit|fund)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["min_deposit"] = int(match.group(1).replace(',', ''))

    match = re.search(r'(?:direct deposit|dd)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["direct_deposit"] = int(match.group(1).replace(',', ''))
    else:
        if re.search(r'direct deposit', text, re.IGNORECASE):
            req["direct_deposit"] = True

    match = re.search(r'(\d+)\s*(?:day|days|month|months)', text, re.IGNORECASE)
    if match:
        num = int(match.group(1))
        unit = match.group(2).lower()
        if 'month' in unit:
            req["holding_days"] = num * 30
        else:
            req["holding_days"] = num

    match = re.search(r'(\d+)\s*(?:debit|purchases|transactions)', text, re.IGNORECASE)
    if match:
        req["transaction_count"] = int(match.group(1))

    match = re.search(r'(?:maintain|keep|balance)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["min_balance"] = int(match.group(1).replace(',', ''))

    state_abbr = r'\b(AK|AL|AR|AZ|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b'
    match = re.findall(state_abbr, text)
    if match:
        req["geographic_restrictions"] = list(set(match))

    date_match = re.search(r'(?:offer ends?|expires?|valid through)[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{1,2}[/\-]\d{1,2})', text, re.IGNORECASE)
    if date_match:
        req["expiration"] = date_match.group(1)

    if "in branch" in text.lower():
        req["notes"].append("in branch only")
    if "no direct deposit" in text.lower():
        req["notes"].append("no direct deposit required")
    if "referral" in text.lower():
        req["notes"].append("referral bonus")

    return req

def parse_common_bonus(text, source_url, category):
    cleaned_text = re.sub(r'^\d+\.\d+\s*', '', text)
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', cleaned_text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
    if not bank or bank == "Unknown":
        fallback = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s|$)', cleaned_text)
        if fallback:
            bank = fallback.group(1).strip()
        else:
            bank = "Unknown"
    amount = extract_amount(text)
    if not amount or not bank or bank == "Unknown":
        return None

    atype = "unknown"
    low = text.lower()
    if any(k in low for k in ['checking', 'check']):
        atype = "checking"
    elif any(k in low for k in ['savings', 'save']):
        atype = "savings"
    elif any(k in low for k in ['business', 'biz']):
        atype = "business"
    elif any(k in low for k in ['referral', 'refer']):
        atype = "referral"
    elif any(k in low for k in ['crypto', 'bitcoin']):
        atype = "crypto"
    elif any(k in low for k in ['miles', 'points']):
        atype = "travel_miles"
    elif any(k in low for k in ['cashback', 'cash back']):
        atype = "cashback"

    req = extract_requirements(text)

    return {
        "bank": bank,
        "bonus_amount": amount,
        "account_type": atype,
        "raw_text": text,
        "category": category,
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        **req
    }

# ======================== ALL CUSTOM PARSERS ========================
# (These are exactly as in your original code – copy them in full)
def parse_doc_bank(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content')
    if not content:
        content = soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "bank")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit: found {len(bonuses)} bonuses")
    return bonuses

def parse_chase(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    full_text = soup.get_text()
    if '$400' in full_text and 'Chase' in full_text:
        bonus = parse_common_bonus("Chase $400 checking bonus with $1,500 direct deposit within 90 days", source_url, "bank")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_bofa(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Bank of America $500 checking bonus tiered: $100 for $2k, $300 for $5k, $500 for $10k+ direct deposits", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_wells_fargo(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Wells Fargo $325 checking bonus with $1,000+ direct deposits within 90 days", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_citibank(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Citibank $325 checking bonus with two direct deposits totaling $3,000 within 90 days", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_capital_one(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Capital One $250 checking bonus with two $500+ direct deposits within 75 days", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_us_bank(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("U.S. Bank checking bonus tiered: $250 for $2k, $350 for $5k, $450 for $8k+ direct deposits", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_pnc(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("PNC checking bonus: $100 for $500 direct deposit (Virtual Wallet) or $400 for $5,000 (Performance Select) within 60 days", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_td_bank(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("TD Bank checking bonus: $200 for $500 direct deposits (Complete) or $300 for $2,500 (Beyond)", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_penn_community_bank(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Penn Community Bank $400 checking bonus: $1,500 direct deposits OR 20 debit card purchases of $20+ within 60 days. PA/NJ only.", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_truist_doc(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Truist $400 checking bonus: one $2,000+ direct deposit within 90 days. AL, AR, GA, FL, IN, KY, MD, MS, NC, NJ, OH, PA, SC, TN, TX, VA, WV, DC.", source_url, "bank")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_truist_business(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Truist Business Checking $400 bonus: $2,000+ deposit and online banking enrollment", source_url, "business_checking")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_first_commonwealth_business(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("First Commonwealth Bank business checking: $300 for $3k+ deposits + 10 debit txns, or $500 for $5k+ deposits + 10 debit txns within 60 days. PA, OH, IN, KY, WV.", source_url, "business_checking")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_union_savings_business(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Union Savings Bank business checking: $305 for Basic (avg $4k+ balance + 15 txns), $506 for Relationship (avg $15k+ balance). CT only. Expires Mar 31, 2026.", source_url, "business_checking")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_golden1_business(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Golden 1 Credit Union business checking: $300 bonus with $5,000 deposit. CA in-branch only. Expires Feb 28, 2026.", source_url, "business_checking")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_penfed(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("PenFed Credit Union checking bonus: $300 for $20k balance or $225 for $15k balance maintained for 123 days. Nationwide.", source_url, "credit_union")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_becu(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("BECU $500 checking bonus: direct deposit $250+ and 30+ debit purchases within 60 days. WA, ID, OR. Expires Apr 10, 2026.", source_url, "credit_union")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_mountain_america(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Mountain America Credit Union $150 checking bonus: direct deposit within 60 days, eStatements required. UT, ID, NV, NM, MT, AZ. Expires Jun 30, 2026.", source_url, "credit_union")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_alliant_rakuten(html, source_url):
    bonuses = []
    bonus = parse_common_bonus("Alliant Credit Union $150 checking bonus via Rakuten: $500+ direct deposit within 30 days. Nationwide.", source_url, "credit_union")
    if bonus:
        bonuses.append(bonus)
    return bonuses

def parse_okx_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_coinbase_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_bitget_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_kraken_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_mexc_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_htx_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_cryptocom_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_bybit_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_robinhood_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "investment")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_webull_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "investment")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doc_investment(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content') or soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "investment")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit (Investment): found {len(bonuses)} bonuses")
    return bonuses

def parse_airbnb_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_uber_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doordash_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doc_referral(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content') or soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "referral")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit (Referral): found {len(bonuses)} bonuses")
    return bonuses

def parse_rakuten_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "retail")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_honey_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "retail")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doc_retail(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content') or soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "retail")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit (Retail): found {len(bonuses)} bonuses")
    return bonuses

def parse_delta_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "travel")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_marriott_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "travel")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doc_travel(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content') or soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text and 'miles' not in text.lower() and 'points' not in text.lower():
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "travel")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit (Travel): found {len(bonuses)} bonuses")
    return bonuses

def parse_swagbucks_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "survey")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_survey_junkie_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "survey")
        if bonus:
            bonuses.append(bonus)
    return bonuses

def parse_doc_survey(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    content = soup.find('div', class_='entry-content') or soup
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        try:
            bonus = parse_common_bonus(text, source_url, "survey")
            if bonus:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit (Survey): found {len(bonuses)} bonuses")
    return bonuses

def parse_zillow_bonus(html, source_url):
    return []

def parse_redfin_bonus(html, source_url):
    return []

def parse_realtor_bonus(html, source_url):
    return []

# ======================== GENERIC SELECTOR EXTRACTOR ========================
def extract_with_selectors(html, extraction_rules, source_url, category):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    container_selector = extraction_rules.get('container', 'body')
    containers = soup.select(container_selector)
    if not containers:
        containers = [soup]

    for container in containers:
        bonus_data = {}
        valid = True
        for field, rule in extraction_rules.get('fields', {}).items():
            selector = rule['selector']
            elem = container.select_one(selector)
            if not elem:
                valid = False
                break
            text = elem.get_text(strip=True)
            if rule.get('type') == 'amount':
                value = extract_amount(text)
                if value is None:
                    valid = False
                    break
                bonus_data[field] = value
            elif rule.get('type') == 'date':
                bonus_data[field] = text
            else:
                bonus_data[field] = text
        if valid and bonus_data:
            bonus_dict = {
                "bank": bonus_data.get('bank', 'Unknown'),
                "bonus_amount": bonus_data.get('bonus_amount', 0),
                "raw_text": bonus_data.get('requirements', ''),
                "category": category,
                "source": source_url,
                "scraped_at": datetime.utcnow().isoformat(),
            }
            if 'expiration' in bonus_data:
                bonus_dict['expiration'] = bonus_data['expiration']
            bonuses.append(bonus_dict)
    return bonuses

# ======================== HEURISTIC FALLBACK EXTRACTOR ========================
def heuristic_extract_bonus(html: str, url: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, 'html.parser')
    # Get page title (if exists)
    title_tag = soup.find('title')
    page_title = title_tag.get_text(strip=True) if title_tag else ""
    
    # Extract all dollar amounts
    text = soup.get_text()
    amounts = re.findall(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', text)
    if not amounts:
        return None
    
    # Take the first amount as the bonus
    amount_str = amounts[0].replace(',', '')
    try:
        amount = int(float(amount_str))
    except:
        return None

    # Determine a plausible bank name
    if page_title:
        # Often the title contains the bank name (e.g., "Chase Bank - Personal Banking")
        bank_guess = page_title.split('|')[0].split('-')[0].strip()
        if len(bank_guess) > 50:
            bank_guess = bank_guess[:50]
    else:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.replace('www.', '').split('.')[0]
        bank_guess = domain.title()

    return {
        "bank": bank_guess,
        "bonus_amount": amount,
        "raw_text": text[:2000],  # store enough text for later cleaning
        "category": "unknown",
        "source": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": ["extracted heuristically"]
    }

# ======================== LOAD SOURCES ========================
def load_sources():
    config_path = os.path.join(os.path.dirname(__file__), 'sources.json')
    if not os.path.exists(config_path):
        return []
    try:
        with open(config_path, 'r') as f:
            data = json.load(f)
        return data.get('sources', [])
    except Exception as e:
        print(f"⚠️ Could not load sources.json: {e}")
        return []

# ======================== CUSTOM PARSERS MAP ========================
CUSTOM_PARSERS = {
    "doc_bank": parse_doc_bank,
    "chase": parse_chase,
    "bofa": parse_bofa,
    "wells_fargo": parse_wells_fargo,
    "citibank": parse_citibank,
    "capital_one": parse_capital_one,
    "us_bank": parse_us_bank,
    "pnc": parse_pnc,
    "td_bank": parse_td_bank,
    "penn_community_bank": parse_penn_community_bank,
    "truist_doc": parse_truist_doc,
    "truist_business": parse_truist_business,
    "first_commonwealth_business": parse_first_commonwealth_business,
    "union_savings_business": parse_union_savings_business,
    "golden1_business": parse_golden1_business,
    "penfed": parse_penfed,
    "becu": parse_becu,
    "mountain_america": parse_mountain_america,
    "alliant_rakuten": parse_alliant_rakuten,
    "okx_bonus": parse_okx_bonus,
    "coinbase_bonus": parse_coinbase_bonus,
    "bitget_bonus": parse_bitget_bonus,
    "kraken_bonus": parse_kraken_bonus,
    "mexc_bonus": parse_mexc_bonus,
    "htx_bonus": parse_htx_bonus,
    "cryptocom_bonus": parse_cryptocom_bonus,
    "bybit_bonus": parse_bybit_bonus,
    "robinhood_bonus": parse_robinhood_bonus,
    "webull_bonus": parse_webull_bonus,
    "doc_investment": parse_doc_investment,
    "airbnb_bonus": parse_airbnb_bonus,
    "uber_bonus": parse_uber_bonus,
    "doordash_bonus": parse_doordash_bonus,
    "doc_referral": parse_doc_referral,
    "rakuten_bonus": parse_rakuten_bonus,
    "honey_bonus": parse_honey_bonus,
    "doc_retail": parse_doc_retail,
    "delta_bonus": parse_delta_bonus,
    "marriott_bonus": parse_marriott_bonus,
    "doc_travel": parse_doc_travel,
    "swagbucks_bonus": parse_swagbucks_bonus,
    "survey_junkie_bonus": parse_survey_junkie_bonus,
    "doc_survey": parse_doc_survey,
    "zillow_bonus": parse_zillow_bonus,
    "redfin_bonus": parse_redfin_bonus,
    "realtor_bonus": parse_realtor_bonus,
}

# ======================== KNOWN BANKS SET ========================
def fetch_known_banks():
    url = "https://en.wikipedia.org/wiki/List_of_banks_(alphabetical)"
    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠️ Could not fetch bank list: {e}. Falling back to empty set.")
        return set()
    soup = BeautifulSoup(response.text, 'html.parser')
    known_banks = set()
    for heading in soup.find_all(['h2', 'h3']):
        ul = heading.find_next('ul')
        if not ul:
            continue
        for li in ul.find_all('li'):
            a = li.find('a')
            if a and a.text.strip():
                name = a.text.strip()
                name = re.sub(r',.*$', '', name)
                name = re.sub(r'\s*\([^)]*\)', '', name)
                known_banks.add(name.lower())
    print(f"✅ Loaded {len(known_banks)} known bank names from Wikipedia.")
    return known_banks

KNOWN_BANKS = fetch_known_banks()

# ======================== BANK SUFFIXES & STOPWORDS ========================
BANK_SUFFIXES = {
    'bank', 'banks', 'credit union', 'financial', 'trust', 'savings', 'federal',
    'community', 'national', 'state', 'cooperative', 'building society', 'banco',
    'bancorp', 'group', 'holdings', 'plc', 'ltd', 'limited', 'incorporated', 'inc',
    'association', 'fund', 'capital', 'partners', 'asset management', 'wealth'
}

COMMON_STOPWORDS_FIRST_WORD = {
    'can', 'just', 'there', 'this', 'has', 'was', 'two', 'No', 'monthly', 'fees', 'to', 'worry', 'about', 'recently', 'increased', 'from', 'direct', 'requires', 'deposit', 
    'bonus', 'offer', 'previously', 'also', 'and', 'the', 'but', 'not', 'so', 'if', 'such', 'as', 'Sometimes', 'it', 'includes', 'or', 'something', 'similar', 'instead',
    'at', 'by', 'for', 'from', 'in', 'into', 'of', 'on', 'to', 'with', 'about', 'above', 'across', 
    'after', 'against', 'along', 'among', 'around', 'before', 'behind', 'below', 'beneath', 'beside', 
    'between', 'beyond', 'down', 'during', 'except', 'like', 'near', 'off', 'onto', 'out', 'outside', 
    'over', 'past', 'since', 'through', 'throughout', 'toward', 'under', 'underneath', 'until', 'up', 
    'upon', 'within', 'without', 'be', 'been', 'being', 'is', 'are', 'was', 'were', 'has', 'have', 'had', 
    'do', 'does', 'did', 'may', 'might', 'must', 'shall', 'should', 'will', 'would', 'could', 'that', 
    'these', 'those', 'it', 'they', 'them', 'he', 'she', 'we', 'you', 'his', 'her', 'its', 'our', 'their', 
    'my', 'your', 'no', 'yes'
}

# ======================== ENHANCED TRANSFORM BONUS ========================
def transform_bonus(old_bonus):
    bonus_amount = old_bonus.get('bonus_amount')
    if not bonus_amount or bonus_amount <= 0:
        return None
    
    raw_text = old_bonus.get('raw_text', '')
    if not raw_text or len(raw_text) < 10:
        return None

    candidate = old_bonus.get('bank') or old_bonus.get('platform') or ''
    candidate_lower = candidate.lower()
    raw_lower = raw_text.lower()

    best_match = None
    max_len = 0
    for bank in KNOWN_BANKS:
        if bank in raw_lower:
            if len(bank) > max_len:
                max_len = len(bank)
                best_match = bank
    if best_match:
        bank_or_platform = best_match.title()
    else:
        phrases_to_remove = [
            r'^(?:can|just|there|this|has|was|two|one|direct|requires|deposit|bonus|offer|previously|also|and|the|but|not|so|if)\s+',
            r'^(?:up to|up to the|fund up to|funding|fund|get|need to|require|required|with|by|for|from|in|into|of|on|to)\s+',
        ]
        cleaned = candidate
        for pattern in phrases_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+(?:bonus|referral|offer|signup|account|checking|savings|business|personal)$', '', cleaned, flags=re.IGNORECASE).strip()
        
        contains_bank_suffix = any(suffix in cleaned.lower() for suffix in BANK_SUFFIXES)
        if not contains_bank_suffix:
            words = cleaned.split()
            for word in words:
                if word.lower() in KNOWN_BANKS:
                    bank_or_platform = word
                    break
            else:
                return None
        else:
            bank_or_platform = cleaned

    if len(bank_or_platform) < 3:
        return None

    capital = old_bonus.get('min_deposit') or old_bonus.get('direct_deposit') or 0
    if isinstance(capital, bool):
        capital = 0
    if not isinstance(capital, int):
        capital = 0

    days = old_bonus.get('holding_days') or 60

    req_count = 0
    if old_bonus.get('min_deposit'): req_count += 1
    if old_bonus.get('direct_deposit'): req_count += 1
    if old_bonus.get('transaction_count'): req_count += 1
    if old_bonus.get('min_balance'): req_count += 1
    difficulty = min(5, max(1, req_count + 1))

    category = old_bonus.get('category') or 'unknown'
    id_str = f"{bank_or_platform}-{bonus_amount}-{category}".lower()
    id_str = re.sub(r'[^a-z0-9]+', '-', id_str).strip('-')

    new_bonus = {
        "id": id_str,
        "bonusAmount": int(bonus_amount),
        "bonusDescription": raw_text[:200],
        "category": category,
        "capitalRequired": capital,
        "estimatedTimeDays": days,
        "difficulty": difficulty,
        "country": "USA",
        "currency": "USD",
        "requirements": raw_text,
        "url": old_bonus.get('source', ''),
        "expiryDate": old_bonus.get('expiration'),
        "tags": [],
        "restrictions": "",
        "terms": "",
        "earlyTerminationFee": None,
        "scrapedAt": old_bonus.get('scraped_at', datetime.utcnow().isoformat()),
        "sourceUrl": old_bonus.get('source', ''),
        "verified": True
    }

    if category in ('bank', 'business_checking', 'credit_union'):
        new_bonus["bank"] = bank_or_platform
        new_bonus["accountType"] = old_bonus.get('account_type', 'checking')
    elif category in ('crypto', 'investment', 'referral', 'retail', 'travel', 'survey', 'real_estate'):
        new_bonus["platform"] = bank_or_platform
        if category == 'crypto':
            new_bonus["cryptoType"] = old_bonus.get('account_type', 'trading')
        elif category == 'investment':
            new_bonus["investmentType"] = old_bonus.get('account_type', 'stocks')

    if old_bonus.get('geographic_restrictions'):
        new_bonus["restrictions"] = "Available in: " + ", ".join(old_bonus['geographic_restrictions'])
        new_bonus["tags"].extend(old_bonus['geographic_restrictions'])

    if old_bonus.get('notes'):
        new_bonus["tags"].extend(old_bonus['notes'])

    return new_bonus

# ======================== DISCOVERY CRAWLER ========================
async def discover_incentive_pages(
    seed_url: str,
    max_pages: int = MAX_PAGES_PER_DOMAIN,
    follow_external: bool = True,
    delay: float = DELAY
) -> Tuple[List[str], Set[str]]:
    visited_pages = set()
    candidates = []
    new_domains = set()
    queue = [seed_url]
    seed_domain = urlparse(seed_url).netloc

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        while queue and len(visited_pages) < max_pages:
            url = queue.pop(0)
            if url in visited_pages:
                continue

            if not await can_fetch(url):
                print(f"🛑 Skipping {url} (disallowed)")
                visited_pages.add(url)
                continue

            visited_pages.add(url)
            print(f"  Crawling {url} ...")

            try:
                await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                text = await page.inner_text("body")
                has_incentive = any(kw in text.lower() for kw in INCENTIVE_KEYWORDS)
                if has_incentive:
                    candidates.append(url)

                links = await page.eval_on_selector_all('a[href]', 'els => els.map(e => e.href)')
                external_count = 0
                for link in links:
                    full_url = urljoin(url, link)
                    parsed = urlparse(full_url)
                    if parsed.netloc == seed_domain:
                        if full_url not in visited_pages and full_url not in queue:
                            queue.append(full_url)
                    elif follow_external and external_count < MAX_EXTERNAL_LINKS_PER_PAGE:
                        external_count += 1
                        new_domains.add(parsed.netloc)

                await asyncio.sleep(delay + random.uniform(0, 1))

            except Exception as e:
                print(f"    ❌ Error: {e}")

        await browser.close()
    return candidates, new_domains

# ======================== PROCESS A SINGLE URL ========================
async def process_url(url: str, known_parsers: Dict) -> List[Dict]:
    if not url or not url.startswith('http'):
        print(f"⚠️ Invalid URL: {url}")
        return []
    if not await can_fetch(url):
        print(f"🛑 Skipping {url} (disallowed by robots.txt)")
        return []

    html = await fetch_url_async(url)
    if html is None:
        html = await fetch_dynamic_async(url)
        if html is None:
            return []

    source_config = None
    for src in load_sources():
        if src.get('url') == url:
            source_config = src
            break

    if source_config and source_config.get('parser') in known_parsers:
        parser = known_parsers[source_config['parser']]
        try:
            bonuses = parser(html, url)
            return bonuses if bonuses else []
        except Exception as e:
            print(f"Custom parser error for {url}: {e}")
            return []
    else:
        bonus = heuristic_extract_bonus(html, url)
        return [bonus] if bonus else []

# ======================== MAIN ORCHESTRATOR ========================
async def run_autonomous_crawler():
    print("🕷️ Starting autonomous global bonus crawler...")
    crawl_start_time = int(datetime.utcnow().timestamp() * 1000)

    all_seeds = load_all_seeds()
    print(f"🌱 Loaded {len(all_seeds)} seed URLs (static + discovered).")

    discovered_urls = set()
    all_new_domains = set()
    for seed in all_seeds:
        print(f"🌱 Crawling seed: {seed}")
        candidates, new_domains = await discover_incentive_pages(
            seed, max_pages=50, follow_external=True
        )
        discovered_urls.update(candidates)
        all_new_domains.update(new_domains)
        print(f"  Found {len(candidates)} candidate pages, {len(new_domains)} new domains.")

    print(f"✅ Total discovered unique URLs: {len(discovered_urls)}")
    print(f"✅ New domains discovered: {len(all_new_domains)}")

    validated_new_domains = set()
    for domain in all_new_domains:
        if not domain:
            continue
        homepage = f"https://{domain}"
        if not await can_fetch(homepage):
            print(f"⚠️ {homepage} disallowed by robots.txt, skipping.")
            continue
        print(f"🔍 Validating new domain: {homepage}")
        html = await fetch_url_async(homepage)
        if html is None:
            html = await fetch_dynamic_async(homepage)
        if html is None:
            continue
        if any(kw in html.lower() for kw in INCENTIVE_KEYWORDS):
            validated_new_domains.add(domain)
            print(f"  ✅ Validated (contains incentives)")
        else:
            print(f"  ❌ No incentives found")

    print(f"✅ Validated new domains: {len(validated_new_domains)}")

    if len(validated_new_domains) > MAX_NEW_DOMAINS_PER_RUN:
        import random
        validated_new_domains = set(random.sample(list(validated_new_domains), MAX_NEW_DOMAINS_PER_RUN))
    save_new_domains(validated_new_domains)

    known_parsers = CUSTOM_PARSERS

    raw_bonuses = []
    all_urls_to_process = set(discovered_urls)
    for src in load_sources():
        url = src.get('url')
        if url:
            all_urls_to_process.add(url)

    for url in all_urls_to_process:
        print(f"🔍 Processing {url}")
        bonuses = await process_url(url, known_parsers)
        raw_bonuses.extend(bonuses)
        await asyncio.sleep(DELAY + random.uniform(0, 1))

    seen = set()
    unique_raw = []
    for b in raw_bonuses:
        key = (b.get('bank') or 'Unknown', b.get('bonus_amount'), b.get('raw_text', '')[:50])
        if key not in seen:
            seen.add(key)
            unique_raw.append(b)

    print(f"After deduplication: {len(unique_raw)} unique bonuses")

    transformed = [transform_bonus(b) for b in unique_raw]
    transformed = [b for b in transformed if b is not None]

    print(f"After validation: {len(transformed)} valid bonuses")

    output = format_output(transformed, crawl_start_time)

    os.makedirs("output", exist_ok=True)
    with open("output/bonuses.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Done. Saved {len(transformed)} bonuses to output/bonuses.json")
    print(f"📦 Output: output/bonuses.json")
    print(f"🌱 New domains added to seed list: {len(validated_new_domains)}")

def format_output(bonuses, crawl_start_time=None):
    categories = list(set(b['category'] for b in bonuses))
    total_value = sum(b['bonusAmount'] for b in bonuses)
    current_time = int(datetime.utcnow().timestamp() * 1000)
    crawl_start = crawl_start_time or current_time

    return {
        "bonuses": bonuses,
        "lastUpdated": datetime.utcnow().isoformat() + "Z",
        "bonusCount": len(bonuses),
        "version": int(datetime.utcnow().timestamp()),
        "categories": categories,
        "sources": list(set(b.get('bank') or b.get('platform') for b in bonuses)),
        "metadata": {
            "lastTriggerTime": crawl_start,
            "lastCrawlingTime": current_time,
            "lastUpdateTime": current_time,
            "newBonusesAdded": len(bonuses),
            "totalValue": total_value,
            "crawlerVersion": "1.0.0"
        },
        "meta": {
            "disclaimer": "Bonus information is gathered from public sources. Please verify with the financial institution.",
            "crawlerVersion": "1.0.0",
            "totalValue": total_value
        }
    }

if __name__ == "__main__":
    asyncio.run(run_autonomous_crawler())
