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
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
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
# Add real sources for each industry. You can add many more later.
SOURCES = {
    "bank": [
        {
            "name": "Doctor of Credit (US Banks)",
            "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/",
            "parser": "doc_bank"
        },
        {
            "name": "MoneySavingExpert (UK Bank Switching)",
            "url": "https://www.moneysavingexpert.com/banking/compare-best-bank-accounts/",
            "parser": "mse_uk_switch"
        },
        {
            "name": "NerdWallet (Bank Bonuses)",
            "url": "https://www.nerdwallet.com/banking/best-bank-bonuses",
            "parser": "nerdwallet_bank"
        }
    ],
    "crypto": [
        {
            "name": "Coinbase",
            "url": "https://www.coinbase.com/join",
            "parser": "coinbase",
            "dynamic": True
        },
        {
            "name": "Binance",
            "url": "https://www.binance.com/en/activity",
            "parser": "binance",
            "dynamic": True
        },
        {
            "name": "Crypto.com",
            "url": "https://crypto.com/exchange",
            "parser": "crypto_com",
            "dynamic": True
        }
    ],
    "investment": [
        {
            "name": "Robinhood",
            "url": "https://robinhood.com/",
            "parser": "robinhood"
        },
        {
            "name": "Webull",
            "url": "https://www.webull.com/activity",
            "parser": "webull"
        }
    ],
    "referral": [
        {
            "name": "Airbnb",
            "url": "https://airbnb.com/invite",
            "parser": "airbnb"
        },
        {
            "name": "Uber",
            "url": "https://uber.com/invite",
            "parser": "uber"
        },
        {
            "name": "DoorDash",
            "url": "https://www.doordash.com/referral",
            "parser": "doordash"
        }
    ],
    "retail": [
        {
            "name": "Rakuten",
            "url": "https://www.rakuten.com/welcome",
            "parser": "rakuten"
        },
        {
            "name": "Honey (PayPal)",
            "url": "https://www.joinhoney.com/",
            "parser": "honey"
        }
    ],
    "travel": [
        {
            "name": "Delta SkyMiles",
            "url": "https://www.delta.com/skymiles-offers",
            "parser": "delta"
        },
        {
            "name": "Marriott Bonvoy",
            "url": "https://www.marriott.com/loyaly",
            "parser": "marriott"
        }
    ],
    "survey": [
        {
            "name": "Swagbucks",
            "url": "https://www.swagbucks.com/offers",
            "parser": "swagbucks"
        },
        {
            "name": "Survey Junkie",
            "url": "https://www.surveyjunkie.com/",
            "parser": "survey_junkie"
        }
    ],
    "uk_switch": [  # kept for backward compatibility
        {
            "name": "MoneySavingExpert (UK Bank Switch)",
            "url": "https://www.moneysavingexpert.com/banking/compare-best-bank-accounts/",
            "parser": "mse_uk_switch"
        }
    ],
    "wealth": [
        {
            "name": "Citi Private Bank",
            "url": "https://www.privatebank.citibank.com/offers",
            "parser": "citi_private"
        }
    ]
}

# ======================== PARSERS ========================
# Each parser takes (html, source_url) and returns a list of bonus dicts.

def parse_doc_bank(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Look for list items with $ in the main content
    for li in soup.select('.entry-content li'):
        text = li.get_text(strip=True)
        if '$' in text:
            bonus = parse_common_bonus(text, source_url, "bank")
            if bonus['bonus_amount']:
                bonuses.append(bonus)
    return bonuses

def parse_mse_uk_switch(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for li in soup.select('li'):
        text = li.get_text()
        if 'switch' in text.lower() and '£' in text:
            bonus = parse_common_bonus(text, source_url, "uk_switch")
            if bonus['bonus_amount']:
                bonuses.append(bonus)
    return bonuses

def parse_nerdwallet_bank(html, source_url):
    # Example parser – adjust selectors based on actual site structure
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for card in soup.select('.bank-offer-card'):
        text = card.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "bank"))
    return bonuses

def parse_coinbase(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Coinbase often uses data-testid or specific classes – inspect the site
    for promo in soup.select('[data-testid="promo-card"], .promo-card, .join-rewards'):
        text = promo.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "crypto"))
    return bonuses

def parse_binance(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Binance promotions – inspect to find correct selectors
    for promo in soup.select('.activity-card, .promotion-item'):
        text = promo.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "crypto"))
    return bonuses

def parse_crypto_com(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for promo in soup.select('.promo-card'):
        text = promo.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "crypto"))
    return bonuses

def parse_robinhood(html, source_url):
    # Placeholder – implement after inspecting robinhood.com
    return []

def parse_webull(html, source_url):
    # Placeholder
    return []

def parse_airbnb(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for link in soup.select('a[href*="invite"]'):
        text = link.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "referral"))
    return bonuses

def parse_uber(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Look for referral promotion text
    for div in soup.select('div'):
        text = div.get_text()
        if 'refer' in text.lower() and '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "referral"))
    return bonuses

def parse_doordash(html, source_url):
    # Placeholder
    return []

def parse_rakuten(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for promo in soup.select('.promo-card'):
        text = promo.get_text()
        if '$' in text or '%' in text:
            bonuses.append(parse_common_bonus(text, source_url, "retail"))
    return bonuses

def parse_honey(html, source_url):
    # Placeholder
    return []

def parse_delta(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for card in soup.select('.offer-card'):
        text = card.get_text()
        if 'miles' in text.lower() or '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "travel"))
    return bonuses

def parse_marriott(html, source_url):
    # Placeholder
    return []

def parse_swagbucks(html, source_url):
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for offer in soup.select('.offer-card'):
        text = offer.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "survey"))
    return bonuses

def parse_survey_junkie(html, source_url):
    # Placeholder
    return []

def parse_citi_private(html, source_url):
    # Placeholder
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
