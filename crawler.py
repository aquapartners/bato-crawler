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
    # This is a placeholder – you can expand for other currencies
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

    # Minimum deposit (e.g., "deposit $500", "$1,000 deposit")
    match = re.search(r'(?:deposit|fund)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["min_deposit"] = int(match.group(1).replace(',', ''))

    # Direct deposit amount (e.g., "direct deposit of $500", "$500 direct deposit")
    match = re.search(r'(?:direct deposit|dd)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["direct_deposit"] = int(match.group(1).replace(',', ''))
    else:
        if re.search(r'direct deposit', text, re.IGNORECASE):
            req["direct_deposit"] = True

    # Holding period (e.g., "90 days", "hold for 3 months")
    match = re.search(r'(\d+)\s*(?:day|days|month|months)', text, re.IGNORECASE)
    if match:
        num = int(match.group(1))
        unit = match.group(2).lower()
        if 'month' in unit:
            req["holding_days"] = num * 30
        else:
            req["holding_days"] = num

    # Transaction count (e.g., "10 debit card transactions")
    match = re.search(r'(\d+)\s*(?:debit|purchases|transactions)', text, re.IGNORECASE)
    if match:
        req["transaction_count"] = int(match.group(1))

    # Minimum balance (e.g., "maintain $1,500 balance")
    match = re.search(r'(?:maintain|keep|balance)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["min_balance"] = int(match.group(1).replace(',', ''))

    # Geographic restrictions – US states
    state_abbr = r'\b(AK|AL|AR|AZ|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b'
    match = re.findall(state_abbr, text)
    if match:
        req["geographic_restrictions"] = list(set(match))

    # Expiration date
    date_match = re.search(r'(?:offer ends?|expires?|valid through)[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{1,2}[/\-]\d{1,2})', text, re.IGNORECASE)
    if date_match:
        req["expiration"] = date_match.group(1)

    # Additional notes
    if "in branch" in text.lower():
        req["notes"].append("in branch only")
    if "no direct deposit" in text.lower():
        req["notes"].append("no direct deposit required")
    if "referral" in text.lower():
        req["notes"].append("referral bonus")

    return req

def parse_common_bonus(text, source_url, category):
    """Parse a single bonus line into structured data."""
    # Try to extract bank name more flexibly
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
    if not bank or bank == "Unknown":
        # fallback: first word(s) until a digit or $ or colon
        fallback = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s|$)', text)
        if fallback:
            bank = fallback.group(1).strip()
    amount = extract_amount(text)

    # Guess account type based on keywords
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

# ======================== SOURCES PER CATEGORY ========================
# For now, focus on bank sources. Others can be added later.
SOURCES = {
    "bank": [
        {
            "name": "Doctor of Credit (US Banks)",
            "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/",
            "parser": "doc_bank"
        },
        # Comment out other sources temporarily to isolate
        # {
        #     "name": "MoneySavingExpert (UK Bank Switching)",
        #     "url": "https://www.moneysavingexpert.com/banking/compare-best-bank-accounts/",
        #     "parser": "mse_uk_switch"
        # },
        # {
        #     "name": "NerdWallet (Bank Bonuses)",
        #     "url": "https://www.nerdwallet.com/banking/best-bank-bonuses",
        #     "parser": "nerdwallet_bank"
        # }
    ],
    # "crypto": [...],  # Commented out for now
    # "investment": [...],
    # "referral": [...],
    # "retail": [...],
    # "travel": [...],
    # "survey": [...],
    # "uk_switch": [...],
    # "wealth": [...]
}

# ======================== PARSERS ========================

def parse_doc_bank(html, source_url):
    """Parse Doctor of Credit by scanning all elements that contain $."""
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Try to find the main content area
    content = soup.find('div', class_='entry-content')
    if not content:
        # fallback to whole page
        content = soup
    # Look for any element (p, li, div) that might contain a bonus
    for elem in content.find_all(['p', 'li', 'div', 'span', 'h3', 'h4']):
        text = elem.get_text(strip=True)
        if not text:
            continue
        # Skip very short or very long texts (likely not a bonus line)
        if len(text) < 10 or len(text) > 300:
            continue
        if '$' not in text:
            continue
        # Skip navigation or footer-like text
        if any(skip in text.lower() for skip in ['copyright', 'privacy', 'terms', 'search']):
            continue
        # Try to parse
        try:
            bonus = parse_common_bonus(text, source_url, "bank")
            if bonus['bonus_amount']:
                bonuses.append(bonus)
                print(f"    Extracted: {bonus['bank']} - ${bonus['bonus_amount']} from: {text[:50]}...")
        except Exception as e:
            print(f"    Error parsing: {text[:50]}... -> {e}")
    print(f"  Found {len(bonuses)} bonuses from Doctor of Credit")
    return bonuses

# Keep placeholder parsers for other sources (they can be empty for now)
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

# Map parser names to functions
PARSERS = {
    "doc_bank": parse_doc_bank,
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

            # Choose fetch method based on 'dynamic' flag
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
        key = (b.get('bank'), b.get('bonus_amount'), b.get('raw_text')[:50])
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
