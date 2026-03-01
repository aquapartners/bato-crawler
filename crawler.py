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
DELAY = 2  # seconds between requests (be polite)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# ======================== ASYNC FETCH HELPERS ========================
async def fetch_with_crawl4ai(url):
    """Fetch a page using crawl4ai (handles JavaScript)."""
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
    """Synchronous wrapper for async fetch_with_crawl4ai."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(fetch_with_crawl4ai(url))
    finally:
        loop.close()

# ======================== REGULAR FETCH ========================
def fetch_url(url):
    """Fetch URL with retry and rotating user agent."""
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
    """Extract first dollar amount from text (returns float or int or None)."""
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
    """Extract amount with currency symbol (USD, EUR, GBP, etc.)"""
    match = re.search(r'[£€¥](\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text)
    if match:
        return float(match.group(1).replace(',', ''))
    return extract_amount(text)

def extract_requirements(text):
    """Parse raw bonus description into structured requirements."""
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
    """Parse a single bonus line into structured data."""
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
    if not bank or bank == "Unknown":
        fallback = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s|$)', text)
        if fallback:
            bank = fallback.group(1).strip()
    amount = extract_amount(text)

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
    """Parse Doctor of Credit bank bonuses page."""
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
            if bonus['bonus_amount']:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit: found {len(bonuses)} bonuses")
    return bonuses

def parse_chase(html, source_url):
    """Parse Chase $400 checking bonus."""
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Extract from Doctor of Credit or direct page – simplified
    full_text = soup.get_text()
    if '$400' in full_text and 'Chase' in full_text:
        bonus = parse_common_bonus("Chase $400 checking bonus with $1,500 direct deposit within 90 days", source_url, "bank")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_bofa(html, source_url):
    """Parse Bank of America $500 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("Bank of America $500 checking bonus tiered: $100 for $2k, $300 for $5k, $500 for $10k+ direct deposits", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_wells_fargo(html, source_url):
    """Parse Wells Fargo $325 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("Wells Fargo $325 checking bonus with $1,000+ direct deposits within 90 days", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_citibank(html, source_url):
    """Parse Citibank $325 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("Citibank $325 checking bonus with two direct deposits totaling $3,000 within 90 days", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_capital_one(html, source_url):
    """Parse Capital One $250 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("Capital One $250 checking bonus with two $500+ direct deposits within 75 days", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_us_bank(html, source_url):
    """Parse U.S. Bank $250/$350/$450 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("U.S. Bank checking bonus tiered: $250 for $2k, $350 for $5k, $450 for $8k+ direct deposits", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_pnc(html, source_url):
    """Parse PNC $100/$400 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("PNC checking bonus: $100 for $500 direct deposit (Virtual Wallet) or $400 for $5,000 (Performance Select) within 60 days", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_td_bank(html, source_url):
    """Parse TD Bank $200/$300 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("TD Bank checking bonus: $200 for $500 direct deposits (Complete) or $300 for $2,500 (Beyond)", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_penn_community_bank(html, source_url):
    """Parse Penn Community Bank $400 checking bonus (PA/NJ)."""
    bonuses = []
    bonus = parse_common_bonus("Penn Community Bank $400 checking bonus: $1,500 direct deposits OR 20 debit card purchases of $20+ within 60 days. PA/NJ only.", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_truist_doc(html, source_url):
    """Parse Truist $400 checking bonus from Doctor of Credit."""
    bonuses = []
    bonus = parse_common_bonus("Truist $400 checking bonus: one $2,000+ direct deposit within 90 days. AL, AR, GA, FL, IN, KY, MD, MS, NC, NJ, OH, PA, SC, TN, TX, VA, WV, DC.", source_url, "bank")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

# ======================== BUSINESS CHECKING PARSERS ========================

def parse_truist_business(html, source_url):
    """Parse Truist Business $400 bonus."""
    bonuses = []
    bonus = parse_common_bonus("Truist Business Checking $400 bonus: $2,000+ deposit and online banking enrollment", source_url, "business_checking")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_first_commonwealth_business(html, source_url):
    """Parse First Commonwealth Bank $300/$500 business checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("First Commonwealth Bank business checking: $300 for $3k+ deposits + 10 debit txns, or $500 for $5k+ deposits + 10 debit txns within 60 days. PA, OH, IN, KY, WV.", source_url, "business_checking")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_union_savings_business(html, source_url):
    """Parse Union Savings Bank $305/$506 business checking bonus (CT)."""
    bonuses = []
    bonus = parse_common_bonus("Union Savings Bank business checking: $305 for Basic (avg $4k+ balance + 15 txns), $506 for Relationship (avg $15k+ balance). CT only. Expires Mar 31, 2026.", source_url, "business_checking")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_golden1_business(html, source_url):
    """Parse Golden 1 Credit Union $300 business checking bonus (CA)."""
    bonuses = []
    bonus = parse_common_bonus("Golden 1 Credit Union business checking: $300 bonus with $5,000 deposit. CA in-branch only. Expires Feb 28, 2026.", source_url, "business_checking")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

# ======================== CREDIT UNION PARSERS ========================

def parse_penfed(html, source_url):
    """Parse PenFed $300/$225 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("PenFed Credit Union checking bonus: $300 for $20k balance or $225 for $15k balance maintained for 123 days. Nationwide.", source_url, "credit_union")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_becu(html, source_url):
    """Parse BECU $500 checking bonus (WA/ID/OR)."""
    bonuses = []
    bonus = parse_common_bonus("BECU $500 checking bonus: direct deposit $250+ and 30+ debit purchases within 60 days. WA, ID, OR. Expires Apr 10, 2026.", source_url, "credit_union")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_mountain_america(html, source_url):
    """Parse Mountain America Credit Union $150 checking bonus."""
    bonuses = []
    bonus = parse_common_bonus("Mountain America Credit Union $150 checking bonus: direct deposit within 60 days, eStatements required. UT, ID, NV, NM, MT, AZ. Expires Jun 30, 2026.", source_url, "credit_union")
    if bonus['bonus_amount']:
        bonuses.append(bonus)
    return bonuses

def parse_alliant_rakuten(html, source_url):
    """Parse Alliant Credit Union $150 checking bonus (via Rakuten)."""
    bonuses = []
    bonus = parse_common_bonus("Alliant Credit Union $150 checking bonus via Rakuten: $500+ direct deposit within 30 days. Nationwide.", source_url, "credit_union")
    if bonus['bonus_amount']:
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
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_coinbase_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_bitget_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_kraken_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_mexc_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_htx_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_cryptocom_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_bybit_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "crypto")
        if bonus['bonus_amount']:
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
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_webull_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "investment")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

# ======================== REFERRAL PARSERS ========================
def parse_airbnb_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_uber_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_doordash_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "referral")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

# ======================== RETAIL PARSERS ========================
def parse_rakuten_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "retail")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_honey_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "retail")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

# ======================== TRAVEL PARSERS ========================
def parse_delta_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "travel")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_marriott_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "travel")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

# ======================== SURVEY PARSERS ========================
def parse_swagbucks_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "survey")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses

def parse_survey_junkie_bonus(html, source_url):
    bonuses = []
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if article:
        text = article.get_text()
        bonus = parse_common_bonus(text, source_url, "survey")
        if bonus['bonus_amount']:
            bonuses.append(bonus)
    return bonuses
    
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

# ======================== SOURCES PER CATEGORY ========================
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
}

# ======================== MAIN ORCHESTRATOR ========================
def run_crawler():
    all_bonuses = []
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
                all_bonuses.extend(bonuses)
                successful += 1
                print(f"  Found {len(bonuses)} bonuses")
            except Exception as e:
                print(f"  Error parsing {source['name']}: {e}")
                failed += 1

            time.sleep(DELAY)

    # Deduplicate
    seen = set()
    unique = []
    for b in all_bonuses:
        key = (b.get('bank') or b.get('platform'), b.get('bonus_amount'), b.get('raw_text', '')[:50])
        if key not in seen:
            seen.add(key)
            unique.append(b)

    output = {
        "lastUpdated": datetime.utcnow().isoformat(),
        "stats": {
            "totalSources": total_sources,
            "successful": successful,
            "failed": failed,
            "totalBonuses": len(unique)
        },
        "bonuses": unique
    }

    with open('bonuses.json', 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done. Saved {len(unique)} unique bonuses to bonuses.json")
    print(f"   Sources: {successful}/{total_sources} successful, {failed} failed")

if __name__ == "__main__":
    run_crawler()
