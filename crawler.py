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
        # Check for generic "direct deposit required" without amount
        if re.search(r'direct deposit', text, re.IGNORECASE):
            req["direct_deposit"] = True  # indicates required, amount unknown

    # Holding period (e.g., "90 days", "hold for 3 months")
    match = re.search(r'(\d+)\s*(?:day|days|month|months)', text, re.IGNORECASE)
    if match:
        num = int(match.group(1))
        unit = match.group(2).lower()
        if 'month' in unit:
            req["holding_days"] = num * 30  # approximate
        else:
            req["holding_days"] = num

    # Transaction count (e.g., "10 debit card transactions", "make 5 purchases")
    match = re.search(r'(\d+)\s*(?:debit|purchases|transactions)', text, re.IGNORECASE)
    if match:
        req["transaction_count"] = int(match.group(1))

    # Minimum balance (e.g., "maintain $1,500 balance")
    match = re.search(r'(?:maintain|keep|balance)[^\d]*\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text, re.IGNORECASE)
    if match:
        req["min_balance"] = int(match.group(1).replace(',', ''))

    # Geographic restrictions (states, counties, etc.)
    # Look for state abbreviations or patterns like "in CA, NV"
    state_abbr = r'\b(AK|AL|AR|AZ|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b'
    match = re.findall(state_abbr, text)
    if match:
        req["geographic_restrictions"] = list(set(match))  # unique states

    # Expiration date – common patterns like "offer ends 12/31/26" or "expires 2026-12-31"
    date_match = re.search(r'(?:offer ends?|expires?|valid through)[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{1,2}[/\-]\d{1,2})', text, re.IGNORECASE)
    if date_match:
        req["expiration"] = date_match.group(1)

    # Additional notes – capture anything that didn't fit
    if "in branch" in text.lower():
        req["notes"].append("in branch only")
    if "no direct deposit" in text.lower():
        req["notes"].append("no direct deposit required")
    if "referral" in text.lower():
        req["notes"].append("referral bonus")

    return req

def parse_common_bonus(text, source_url, category):
    """Parse a single bonus line into structured data with requirements."""
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
    amount = extract_amount(text)

    # Guess account type
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

    # Extract requirements
    req = extract_requirements(text)

    return {
        "bank": bank,
        "bonus_amount": amount,
        "account_type": atype,
        "raw_text": text,
        "category": category,
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        **req  # include all requirement fields at the top level
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
    """Parse Doctor of Credit by scanning all list items for dollar amounts."""
    soup = BeautifulSoup(html, 'html.parser')
    bonuses = []

    # Find all <li> tags anywhere on the page
    all_lis = soup.find_all('li')
    print(f"  Found {len(all_lis)} total <li> tags on page")

    for li in all_lis:
        text = li.get_text(strip=True)
        if not text or '$' not in text:
            continue

        # Use the common parser to extract fields – catch exceptions
        try:
            bonus = parse_common_bonus(text, source_url, "bank")
            if bonus['bonus_amount'] is not None:
                bonuses.append(bonus)
            else:
                print(f"    Skipping (no amount): {text[:80]}...")
        except Exception as e:
            print(f"    Error parsing line: {text[:80]}... - {e}")
            continue

    print(f"  Extracted {len(bonuses)} bonuses from Doctor of Credit")
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
