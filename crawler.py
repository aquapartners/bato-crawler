import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
from urllib.parse import urlparse

# ======================== CONFIGURATION ========================
DELAY = 2  # seconds between requests (be polite)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# ======================== HELPER FUNCTIONS ========================
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

def extract_amount(text):
    """Extract first dollar amount from text (returns int or None)."""
    match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text)
    if match:
        return int(match.group(1).replace(',', ''))
    return None

def parse_common_bonus(text, source_url, category):
    """Fallback parser for simple description lines."""
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    amount = extract_amount(text)
    # Guess account type from keywords
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
    return {
        "bank": bank,
        "bonus_amount": amount,
        "account_type": atype,
        "raw_text": text,
        "category": category,
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat()
    }

# ======================== SOURCES PER CATEGORY ========================
SOURCES = {
    "bank": [
        {
            "name": "Doctor of Credit",
            "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/",
            "parser": "doc_bank"
        },
        # Add more bank sources here
    ],
    "crypto": [
        {
            "name": "Coinbase",
            "url": "https://www.coinbase.com/join",
            "parser": "coinbase"
        },
        {
            "name": "Binance",
            "url": "https://www.binance.com/en/activity",
            "parser": "binance"
        },
        # Add more crypto sources
    ],
    "investment": [
        {
            "name": "Robinhood",
            "url": "https://robinhood.com/",
            "parser": "robinhood"
        },
        # Add more
    ],
    "referral": [
        {
            "name": "Airbnb",
            "url": "https://airbnb.com/invite",
            "parser": "airbnb"
        },
        # Add more
    ],
    "retail": [
        {
            "name": "Rakuten",
            "url": "https://www.rakuten.com/welcome",
            "parser": "rakuten"
        },
        # Add more
    ],
    "travel": [
        {
            "name": "Delta SkyMiles",
            "url": "https://www.delta.com/skymiles-offers",
            "parser": "delta"
        },
        # Add more
    ],
    "survey": [
        {
            "name": "Swagbucks",
            "url": "https://www.swagbucks.com/offers",
            "parser": "swagbucks"
        },
        # Add more
    ],
    "uk_switch": [
        {
            "name": "MoneySavingExpert",
            "url": "https://www.moneysavingexpert.com/banking/compare-best-bank-accounts/",
            "parser": "mse"
        },
        # Add more
    ],
    "wealth": [
        {
            "name": "Citi Private Bank",
            "url": "https://www.privatebank.citibank.com/offers",
            "parser": "citi_private"
        },
        # Add more
    ],
}

# ======================== PARSERS FOR EACH SOURCE ========================
def parse_doc_bank(html, source_url):
    """Parse Doctor of Credit bank bonuses page."""
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for article in soup.select('article'):
        title = article.select_one('.entry-title')
        if title and 'bank' in title.text.lower():
            for li in article.select('li'):
                text = li.get_text(strip=True)
                if text and '$' in text:
                    bonuses.append(parse_common_bonus(text, source_url, "bank"))
    return bonuses

def parse_coinbase(html, source_url):
    """Parse Coinbase join page (example)."""
    # This is a placeholder – real implementation would inspect the page.
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    # Example: look for promo sections
    for promo in soup.select('.promo-card'):
        text = promo.get_text()
        if '$' in text:
            bonuses.append(parse_common_bonus(text, source_url, "crypto"))
    return bonuses

def parse_binance(html, source_url):
    # Placeholder
    return []

def parse_robinhood(html, source_url):
    # Placeholder
    return []

def parse_airbnb(html, source_url):
    # Placeholder
    return []

def parse_rakuten(html, source_url):
    # Placeholder
    return []

def parse_delta(html, source_url):
    # Placeholder
    return []

def parse_swagbucks(html, source_url):
    # Placeholder
    return []

def parse_mse(html, source_url):
    """Parse MoneySavingExpert bank switching page."""
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []
    for li in soup.select('li'):
        text = li.get_text()
        if 'switch' in text.lower() and '£' in text:
            # Convert £ to USD roughly (optional)
            bonuses.append(parse_common_bonus(text, source_url, "uk_switch"))
    return bonuses

def parse_citi_private(html, source_url):
    # Placeholder
    return []

# Map parser names to functions
PARSERS = {
    "doc_bank": parse_doc_bank,
    "coinbase": parse_coinbase,
    "binance": parse_binance,
    "robinhood": parse_robinhood,
    "airbnb": parse_airbnb,
    "rakuten": parse_rakuten,
    "delta": parse_delta,
    "swagbucks": parse_swagbucks,
    "mse": parse_mse,
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
            time.sleep(DELAY)  # be polite

    # Deduplicate (by raw_text + bank + amount)
    seen = set()
    unique = []
    for b in all_bonuses:
        key = (b.get('bank'), b.get('bonus_amount'), b.get('raw_text')[:50])
        if key not in seen:
            seen.add(key)
            unique.append(b)

    # Prepare final output
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

    # Save to file
    with open('bonuses.json', 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done. Saved {len(unique)} unique bonuses to bonuses.json")
    print(f"   Sources: {successful}/{total_sources} successful, {failed} failed")

if __name__ == "__main__":
    run_crawler()
