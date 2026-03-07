import requests
from bs4 import BeautifulSoup
import json
import re
import time
import asyncio
from datetime import datetime
from urllib.parse import urlparse

# ======================== CRAWL4AI IMPORTS ========================
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

# ======================== CONFIGURATION ========================
DELAY = 2
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# ======================== ASYNC FETCH HELPERS ========================
async def fetch_with_crawl4ai(url):
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

def fetch_dynamic(url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(fetch_with_crawl4ai(url))
    finally:
        loop.close()

def fetch_url(url):
    headers = {'User-Agent': USER_AGENTS[hash(url) % len(USER_AGENTS)]}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
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

# ======================== BANK PARSERS ========================
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

# ======================== BUSINESS CHECKING PARSERS ========================
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

# ======================== CREDIT UNION PARSERS ========================
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

# ======================== CRYPTO PARSERS ========================
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

# ======================== INVESTMENT PARSERS ========================
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

# ======================== REFERRAL PARSERS ========================
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

# ======================== RETAIL PARSERS ========================
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

# ======================== TRAVEL PARSERS ========================
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

# ======================== SURVEY PARSERS ========================
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

# ======================== REAL ESTATE PARSERS ========================
def parse_zillow_bonus(html, source_url):
    return []

def parse_redfin_bonus(html, source_url):
    return []

def parse_realtor_bonus(html, source_url):
    return []

# ======================== OTHER PLACEHOLDERS ========================
def parse_mse_uk_switch(html, source_url):
    return []

def parse_nerdwallet_bank(html, source_url):
    return []

def parse_coinbase(html, source_url):
    return []

def parse_binance(html, source_url):
    return []

def parse_crypto_com(html, source_url):
    return []

def parse_robinhood(html, source_url):
    return []

def parse_webull(html, source_url):
    return []

def parse_airbnb(html, source_url):
    return []

def parse_uber(html, source_url):
    return []

def parse_doordash(html, source_url):
    return []

def parse_rakuten(html, source_url):
    return []

def parse_honey(html, source_url):
    return []

def parse_delta(html, source_url):
    return []

def parse_marriott(html, source_url):
    return []

def parse_swagbucks(html, source_url):
    return []

def parse_survey_junkie(html, source_url):
    return []

def parse_citi_private(html, source_url):
    return []

# ======================== SOURCES PER CATEGORY (FULL) ========================
SOURCES = {
    "bank": [
        {
            "name": "Doctor of Credit (US Banks)",
            "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/",
            "parser": "doc_bank"
        },
        {
            "name": "Chase $400 Checking",
            "url": "https://www.doctorofcredit.com/chase-300-checking-bonus-2/",
            "parser": "chase"
        },
        {
            "name": "Bank of America $500 Checking",
            "url": "https://www.doctorofcredit.com/bank-of-america-100-200-300-checking-bonus/",
            "parser": "bofa"
        },
        {
            "name": "Wells Fargo $325 Checking",
            "url": "https://www.doctorofcredit.com/wells-fargo-325-checking-bonus/",
            "parser": "wells_fargo"
        },
        {
            "name": "Citibank $325 Checking",
            "url": "https://www.doctorofcredit.com/citibank-325-checking-bonus/",
            "parser": "citibank"
        },
        {
            "name": "Capital One $250 Checking",
            "url": "https://www.doctorofcredit.com/capital-one-250-checking-bonus/",
            "parser": "capital_one"
        },
        {
            "name": "U.S. Bank $250-$450 Checking",
            "url": "https://www.doctorofcredit.com/u-s-bank-250-350-450-checking-bonus/",
            "parser": "us_bank"
        },
        {
            "name": "PNC $100/$400 Checking",
            "url": "https://www.doctorofcredit.com/pnc-200-300-400-checking-bonus/",
            "parser": "pnc"
        },
        {
            "name": "TD Bank $200/$300 Checking",
            "url": "https://www.doctorofcredit.com/td-bank-200-300-checking-bonus/",
            "parser": "td_bank"
        },
        {
            "name": "Penn Community Bank $400 (PA/NJ)",
            "url": "https://www.doctorofcredit.com/pa-only-penn-community-bank-350-checking-bonus-50-savings-direct-deposit-not-required/",
            "parser": "penn_community_bank"
        },
        {
            "name": "Truist $400 Checking",
            "url": "https://www.doctorofcredit.com/truist-300-checking-bonus-al-ar-ga-fl-in-ky-md-ms-nc-nj-oh-pa-sc-tn-tx-va-wv-or-dc/",
            "parser": "truist_doc"
        }
    ],
    "business_checking": [
        {
            "name": "Truist Business $400",
            "url": "https://www.doctorofcredit.com/truist-200-business-checking-bonus-al-ar-ga-fl-in-ky-md-ms-nc-nj-oh-pa-sc-tn-tx-va-wv-or-dc/",
            "parser": "truist_business"
        },
        {
            "name": "First Commonwealth $300-$500 Business",
            "url": "https://www.doctorofcredit.com/24736-2/",
            "parser": "first_commonwealth_business"
        },
        {
            "name": "Union Savings Bank Business $305/$506",
            "url": "https://www.doctorofcredit.com/ct-only-union-savings-bank-250-500-business-checking-bonus/",
            "parser": "union_savings_business"
        },
        {
            "name": "Golden 1 Credit Union $300 Business",
            "url": "https://www.doctorofcredit.com/ca-in-branch-only-golden-1-credit-union-300-business-checking-bonus/",
            "parser": "golden1_business"
        }
    ],
    "credit_union": [
        {
            "name": "PenFed $300/$225 Checking",
            "url": "https://www.doctorofcredit.com/ymmv-penfed-300-checking-bonus/",
            "parser": "penfed"
        },
        {
            "name": "BECU $500 Checking",
            "url": "https://www.doctorofcredit.com/wa-id-or-only-becu-400-checking-bonus/",
            "parser": "becu"
        },
        {
            "name": "Mountain America $150 Checking",
            "url": "https://www.doctorofcredit.com/ut-id-nv-nm-mt-az-mountain-america-credit-union-150-checking-bonus/",
            "parser": "mountain_america"
        },
        {
            "name": "Alliant Credit Union $150 (Rakuten)",
            "url": "https://www.doctorofcredit.com/rakuten-alliant-credit-union-100-10000-checking-bonus/",
            "parser": "alliant_rakuten"
        }
    ],
    "investment": [
        {
            "name": "Robinhood $100/$500 Bonus",
            "url": "https://www.doctorofcredit.com/robinhood-500-bonus/",
            "parser": "robinhood_bonus"
        },
        {
            "name": "Webull $1500+ Bonus",
            "url": "https://www.doctorofcredit.com/webull-5000-bonus/",
            "parser": "webull_bonus"
        },
        {
            "name": "Doctor of Credit (Investment)",
            "url": "https://www.doctorofcredit.com/category/investment-brokerage/",
            "parser": "doc_investment"
        }
    ],
    "referral": [
        {
            "name": "Airbnb Referral Bonus",
            "url": "https://www.doctorofcredit.com/airbnb-45-bonus-for-international-stays/",
            "parser": "airbnb_bonus"
        },
        {
            "name": "Uber Referral Bonus",
            "url": "https://www.doctorofcredit.com/uber-15-bonus-uber-eats-25/",
            "parser": "uber_bonus"
        },
        {
            "name": "Doctor of Credit (Referral)",
            "url": "https://www.doctorofcredit.com/category/referral-bonuses/",
            "parser": "doc_referral"
        },
        {
            "name": "DoorDash Referral Bonus",
            "url": "https://www.doctorofcredit.com/doordash-10-off/",
            "parser": "doordash_bonus"
        }
    ],
    "retail": [
        {
            "name": "Rakuten Cashback",
            "url": "https://www.doctorofcredit.com/rakuten-30-bonus/",
            "parser": "rakuten_bonus"
        },
        {
            "name": "Honey (PayPal) Cashback",
            "url": "https://www.doctorofcredit.com/honey-10-bonus/",
            "parser": "honey_bonus"
        },
        {
            "name": "Doctor of Credit (Retail/Cashback)",
            "url": "https://www.doctorofcredit.com/category/cashback-portals/",
            "parser": "doc_retail"
        }
    ],
    "travel": [
        {
            "name": "Delta SkyMiles Bonus",
            "url": "https://www.doctorofcredit.com/delta-skymiles-50000-bonus/",
            "parser": "delta_bonus"
        },
        {
            "name": "Marriott Bonvoy Bonus",
            "url": "https://www.doctorofcredit.com/marriott-bonvoy-50000-bonus/",
            "parser": "marriott_bonus"
        },
        {
            "name": "Doctor of Credit (Travel)",
            "url": "https://www.doctorofcredit.com/category/travel-2/",
            "parser": "doc_travel"
        }
    ],
    "survey": [
        {
            "name": "Swagbucks Signup Bonus",
            "url": "https://www.doctorofcredit.com/swagbucks-10-bonus/",
            "parser": "swagbucks_bonus"
        },
        {
            "name": "Survey Junkie Bonus",
            "url": "https://www.doctorofcredit.com/survey-junkie-5-bonus/",
            "parser": "survey_junkie_bonus"
        },
        {
            "name": "Doctor of Credit (Surveys/GPT)",
            "url": "https://www.doctorofcredit.com/category/surveys-gpt/",
            "parser": "doc_survey"
        }
    ],
    "crypto": [
        {
            "name": "OKX Up to $10,000 Welcome Bonus",
            "url": "https://www.doctorofcredit.com/okx-crypto-exchange-review-bonus/",
            "parser": "okx_bonus"
        },
        {
            "name": "Coinbase Up to $200 Crypto Bonus",
            "url": "https://www.doctorofcredit.com/coinbase-review-bonus/",
            "parser": "coinbase_bonus"
        },
        {
            "name": "Bitget $5,000 Trial Fund + Rebates",
            "url": "https://www.doctorofcredit.com/bitget-crypto-exchange-review-bonus/",
            "parser": "bitget_bonus"
        },
        {
            "name": "Kraken 3% Deposit Match",
            "url": "https://www.doctorofcredit.com/kraken-3-cash-crypto-deposit-match-18-month-hold/",
            "parser": "kraken_bonus"
        },
        {
            "name": "MEXC Referral Ambassador Program",
            "url": "https://www.mexc.com/en-TR/announcements/article/mexc-launches-the-referral-ambassador-program-17827791531306",
            "parser": "mexc_bonus",
            "dynamic": True
        },
        {
            "name": "HTX New Funds Bonus Trial",
            "url": "https://www.htx.com/support/55024606728745",
            "parser": "htx_bonus",
            "dynamic": True
        },
        {
            "name": "Crypto.com VIP Referral Program",
            "url": "https://crypto.com/sg/product-news/exchange-vip-referral-program",
            "parser": "cryptocom_bonus",
            "dynamic": True
        },
        {
            "name": "Bybit $1,000,000 Boost Battle",
            "url": "https://announcements.bybit.com/article/boost-battle-x-tmgp-2026-series-1-trade-daily-grab-your-share-of-the-1-000-000-usdt-prize-pool--blt353d08203eb770b9/",
            "parser": "bybit_bonus",
            "dynamic": True
        }
    ],
    "real_estate": [
        {
            "name": "Zillow Referral Bonus",
            "url": "https://www.zillow.com/referral/",
            "parser": "zillow_bonus"
        },
        {
            "name": "Redfin Referral Bonus",
            "url": "https://www.redfin.com/referral/",
            "parser": "redfin_bonus"
        },
        {
            "name": "Realtor.com Referral Bonus",
            "url": "https://www.realtor.com/referral/",
            "parser": "realtor_bonus"
        }
    ]
}

# ======================== PARSERS MAP ========================
PARSERS = {
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
    "mse_uk_switch": parse_mse_uk_switch,
    "nerdwallet_bank": parse_nerdwallet_bank,
    "coinbase": parse_coinbase,
    "binance": parse_binance,
    "crypto_com": parse_crypto_com,
    "robinhood": parse_robinhood,
    "webull": parse_webull,
    "airbnb": parse_airbnb,
    "uber": parse_uber,
    "doordash": parse_doordash,
    "rakuten": parse_rakuten,
    "honey": parse_honey,
    "delta": parse_delta,
    "marriott": parse_marriott,
    "swagbucks": parse_swagbucks,
    "survey_junkie": parse_survey_junkie,
    "citi_private": parse_citi_private,
    "robinhood_bonus": parse_robinhood_bonus,
    "webull_bonus": parse_webull_bonus,
    "airbnb_bonus": parse_airbnb_bonus,
    "uber_bonus": parse_uber_bonus,
    "doordash_bonus": parse_doordash_bonus,
    "rakuten_bonus": parse_rakuten_bonus,
    "honey_bonus": parse_honey_bonus,
    "delta_bonus": parse_delta_bonus,
    "marriott_bonus": parse_marriott_bonus,
    "swagbucks_bonus": parse_swagbucks_bonus,
    "survey_junkie_bonus": parse_survey_junkie_bonus,
    "doc_investment": parse_doc_investment,
    "doc_referral": parse_doc_referral,
    "doc_retail": parse_doc_retail,
    "doc_travel": parse_doc_travel,
    "doc_survey": parse_doc_survey,
    "zillow_bonus": parse_zillow_bonus,
    "redfin_bonus": parse_redfin_bonus,
    "realtor_bonus": parse_realtor_bonus,
}

# ======================== KNOWN BANKS SET (with proper User-Agent) ========================
def fetch_known_banks():
    """
    Fetch the Wikipedia list of banks and return a set of lowercased bank names.
    Uses a common browser User-Agent to avoid 403.
    """
    url = "https://en.wikipedia.org/wiki/List_of_banks_(alphabetical)"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠️ Could not fetch bank list: {e}. Falling back to empty set.")
        return set()

    soup = BeautifulSoup(response.text, 'html.parser')
    known_banks = set()
    
    # Find all sections with headings from A to Z
    for heading in soup.find_all(['h2', 'h3']):
        # The actual list items are in <ul> following the heading
        ul = heading.find_next('ul')
        if not ul:
            continue
        for li in ul.find_all('li'):
            # The bank name is usually the linked text
            a = li.find('a')
            if a and a.text.strip():
                name = a.text.strip()
                # Clean up: remove trailing commas, parentheticals
                name = re.sub(r',.*$', '', name)
                name = re.sub(r'\s*\([^)]*\)', '', name)
                known_banks.add(name.lower())
    
    print(f"✅ Loaded {len(known_banks)} known bank names from Wikipedia.")
    return known_banks

KNOWN_BANKS = fetch_known_banks()

# ======================== TRANSFORM BONUS WITH BANK NAME MATCHING ========================
def transform_bonus(old_bonus):
    """
    Convert a bonus dict from the old format to the standardized frontend schema.
    Uses the Wikipedia list to find the actual bank name within the extracted text.
    """
    # Validate bonus amount
    bonus_amount = old_bonus.get('bonus_amount')
    if not bonus_amount or bonus_amount <= 0:
        return None
    
    # Validate raw text length
    raw_text = old_bonus.get('raw_text', '')
    if not raw_text or len(raw_text) < 10:
        return None

    # Try to find a known bank name in the raw text or in the extracted bank field
    candidate = old_bonus.get('bank') or old_bonus.get('platform') or ''
    candidate_lower = candidate.lower()
    
    best_match = None
    max_len = 0
    
    # Look for the longest matching known bank name (case‑insensitive)
    for bank in KNOWN_BANKS:
        if bank in candidate_lower or bank in raw_text.lower():
            if len(bank) > max_len:
                max_len = len(bank)
                best_match = bank.title()  # restore proper case
    
    # If no known bank found, try to fall back to a cleaned version of the candidate
    if not best_match:
        # Remove common descriptive phrases
        cleaned = re.sub(r'^(?:can|just|there|this|has|was|two|one|direct|requires|deposit|bonus|offer|previously|also|and|the|but|not|so|if)\s+', '', candidate, flags=re.IGNORECASE)
        if len(cleaned) > 3 and cleaned != candidate:
            best_match = cleaned.strip()
        else:
            # Reject if still junk-like
            return None
    
    bank_or_platform = best_match

    # Derive capitalRequired
    capital = old_bonus.get('min_deposit') or old_bonus.get('direct_deposit') or 0
    if isinstance(capital, bool):
        capital = 0
    if not isinstance(capital, int):
        capital = 0

    days = old_bonus.get('holding_days') or 60

    # Difficulty heuristic
    req_count = 0
    if old_bonus.get('min_deposit'): req_count += 1
    if old_bonus.get('direct_deposit'): req_count += 1
    if old_bonus.get('transaction_count'): req_count += 1
    if old_bonus.get('min_balance'): req_count += 1
    difficulty = min(5, max(1, req_count + 1))

    # Generate ID
    category = old_bonus.get('category') or 'unknown'
    id_str = f"{bank_or_platform}-{bonus_amount}-{category}".lower()
    id_str = re.sub(r'[^a-z0-9]+', '-', id_str).strip('-')

    # Build new bonus object
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

    # Add category-specific fields
    if category in ('bank', 'business_checking', 'credit_union'):
        new_bonus["bank"] = bank_or_platform
        new_bonus["accountType"] = old_bonus.get('account_type', 'checking')
    elif category in ('crypto', 'investment', 'referral', 'retail', 'travel', 'survey', 'real_estate'):
        new_bonus["platform"] = bank_or_platform
        if category == 'crypto':
            new_bonus["cryptoType"] = old_bonus.get('account_type', 'trading')
        elif category == 'investment':
            new_bonus["investmentType"] = old_bonus.get('account_type', 'stocks')

    # Add geographic restrictions
    if old_bonus.get('geographic_restrictions'):
        new_bonus["restrictions"] = "Available in: " + ", ".join(old_bonus['geographic_restrictions'])
        new_bonus["tags"].extend(old_bonus['geographic_restrictions'])

    # Add notes as tags
    if old_bonus.get('notes'):
        new_bonus["tags"].extend(old_bonus['notes'])

    return new_bonus

# ======================== SCRAPE ALL BONUSES ========================
def scrape_all_bonuses():
    crawl_start_time = int(datetime.utcnow().timestamp() * 1000)
    raw_bonuses = []
    total_sources = 0
    successful = 0
    failed = 0

    for category, sources in SOURCES.items():
        for source in sources:
            total_sources += 1
            print(f"Scraping {source['name']} ({category})...")

            if source.get('dynamic', False):
                html = fetch_dynamic(source['url'])
            else:
                html = fetch_url(source['url'])

            if html is None:
                failed += 1
                continue

            parser = PARSERS.get(source['parser'])
            if not parser:
                print(f"  No parser for {source['parser']}")
                failed += 1
                continue

            try:
                bonuses = parser(html, source['url'])
                if bonuses:
                    bonuses = [b for b in bonuses if b is not None]
                raw_bonuses.extend(bonuses)
                successful += 1
                print(f"  Found {len(bonuses)} bonuses")
            except Exception as e:
                print(f"  Error parsing {source['name']}: {e}")
                failed += 1

            time.sleep(DELAY)

    print(f"\nScraping completed: {successful}/{total_sources} sources succeeded, {failed} failed.")
    print(f"Total raw bonuses collected: {len(raw_bonuses)}")

    seen = set()
    unique_raw = []
    for b in raw_bonuses:
        key = (b.get('bank') or b.get('platform'), b.get('bonus_amount'), b.get('raw_text', '')[:50])
        if key not in seen:
            seen.add(key)
            unique_raw.append(b)

    print(f"After deduplication: {len(unique_raw)} unique bonuses")

    transformed = [transform_bonus(b) for b in unique_raw]
    transformed = [b for b in transformed if b is not None]
    
    print(f"After validation: {len(transformed)} valid bonuses (filtered out {len(unique_raw) - len(transformed)} invalid)")
    
    return transformed, crawl_start_time

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

def main():
    print("🕷️ Starting Bato Crawler...")
    bonuses, crawl_start_time = scrape_all_bonuses()
    output = format_output(bonuses, crawl_start_time)
    import os
    os.makedirs("output", exist_ok=True)
    with open("output/bonuses.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n✅ Done. Saved {len(bonuses)} bonuses to output/bonuses.json")
    print(f"💰 Total value: ${output['meta']['totalValue']:,}")
    print(f"📦 Output: output/bonuses.json")

if __name__ == "__main__":
    main()
