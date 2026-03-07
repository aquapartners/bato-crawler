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
        unit = match.group(2).lower() if len(match.groups()) > 1 else ''
        if 'month' in text.lower():
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
    # Strip leading numbers and dots (e.g., "1.1 ", "2.3 ", etc.)
    cleaned_text = re.sub(r'^\d+\.\d+\s*', '', text)
    
    # Now try to match the bank name from the cleaned text
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', cleaned_text)
    bank = bank_match.group(1).strip() if bank_match else None
    if bank:
        bank = re.sub(r'[\.\:]+$', '', bank).strip()
    
    # If still no bank, try fallback on cleaned text
    if not bank:
        fallback = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s|$)', cleaned_text)
        if fallback:
            bank = fallback.group(1).strip()
    
    amount = extract_amount(text)  # Keep original text for amount extraction
    
    # ✅ FIX: Don't create bonus if we don't have required data
    if not amount or not bank:
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

# ======================== TRANSFORM BONUS (FIXED) ========================

def transform_bonus(old_bonus):
    """
    Convert a bonus dict from the old format to the standardized frontend schema.
    ✅ FIXED: Properly validate and filter out invalid bonuses
    """
    # ✅ FIX 1: Extract bonus_amount and validate it's a real number > 0
    bonus_amount = old_bonus.get('bonus_amount')
    if not bonus_amount or bonus_amount <= 0:
        return None  # Skip invalid bonuses
    
    # ✅ FIX 2: Extract bank/platform name and validate it exists
    bank_or_platform = old_bonus.get('bank') or old_bonus.get('platform')
    if not bank_or_platform or bank_or_platform.lower() in ['unknown', '', 'none']:
        return None  # Skip bonuses without valid bank name
    
    # ✅ FIX 3: Validate we have actual bonus description
    raw_text = old_bonus.get('raw_text', '')
    if not raw_text or len(raw_text) < 10:
        return None  # Skip bonuses without valid description
    
    # Derive capitalRequired from min_deposit or direct_deposit (if numeric)
    capital = old_bonus.get('min_deposit') or old_bonus.get('direct_deposit') or 0
    if isinstance(capital, bool):
        capital = 0  # True means it's required but amount unknown
    if not isinstance(capital, int):
        capital = 0

    # Estimated time: holding_days or default 60
    days = old_bonus.get('holding_days') or 60

    # Difficulty: simple heuristic based on number of requirements
    req_count = 0
    if old_bonus.get('min_deposit'): req_count += 1
    if old_bonus.get('direct_deposit'): req_count += 1
    if old_bonus.get('transaction_count'): req_count += 1
    if old_bonus.get('min_balance'): req_count += 1
    difficulty = min(5, max(1, req_count + 1))  # 1 = very easy, 5 = very hard

    # Generate a unique ID
    category = old_bonus.get('category') or 'unknown'
    id_str = f"{bank_or_platform}-{bonus_amount}-{category}".lower()
    id_str = re.sub(r'[^a-z0-9]+', '-', id_str).strip('-')

    # Build new bonus object with proper validation
    new_bonus = {
        "id": id_str,
        "bonusAmount": int(bonus_amount),  # Ensure it's an integer
        "bonusDescription": raw_text[:200],  # First 200 chars of description
        "category": category,
        "capitalRequired": capital,
        "estimatedTimeDays": days,
        "difficulty": difficulty,
        "country": "USA",  # default, can be overridden by geographic_restrictions
        "currency": "USD",  # default currency
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

    # Add geographic restrictions as tags or separate field
    if old_bonus.get('geographic_restrictions'):
        new_bonus["restrictions"] = "Available in: " + ", ".join(old_bonus['geographic_restrictions'])
        new_bonus["tags"].extend(old_bonus['geographic_restrictions'])

    # Add notes as tags
    if old_bonus.get('notes'):
        new_bonus["tags"].extend(old_bonus['notes'])

    return new_bonus

def scrape_all_bonuses():
    """
    Run all source parsers and collect bonuses, then transform to frontend format.
    ✅ FIXED: Filter out None results from transform_bonus
    """
    # Record when crawling started (in milliseconds for JavaScript compatibility)
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
                # ✅ FIX: Filter out None results from parse_common_bonus
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

    # Deduplicate raw bonuses (by bank+amount+raw_text)
    seen = set()
    unique_raw = []
    for b in raw_bonuses:
        key = (b.get('bank') or b.get('platform'), b.get('bonus_amount'), b.get('raw_text', '')[:50])
        if key not in seen:
            seen.add(key)
            unique_raw.append(b)

    print(f"After deduplication: {len(unique_raw)} unique bonuses")

    # Transform each bonus to frontend format
    # ✅ FIX: Filter out None results from transform_bonus
    transformed = [transform_bonus(b) for b in unique_raw]
    transformed = [b for b in transformed if b is not None]
    
    print(f"After validation: {len(transformed)} valid bonuses (filtered out {len(unique_raw) - len(transformed)} invalid)")
    
    return transformed, crawl_start_time

def format_output(bonuses, crawl_start_time=None):
    """Create the final JSON structure for the frontend."""
    categories = list(set(b['category'] for b in bonuses))
    total_value = sum(b['bonusAmount'] for b in bonuses)
    
    # Calculate timestamps
    current_time = int(datetime.utcnow().timestamp() * 1000)  # milliseconds
    crawl_start = crawl_start_time or current_time
    
    return {
        "bonuses": bonuses,
        "lastUpdated": datetime.utcnow().isoformat() + "Z",
        "bonusCount": len(bonuses),
        "version": int(datetime.utcnow().timestamp()),
        "categories": categories,
        "sources": list(set(b.get('bank') or b.get('platform') for b in bonuses)),
        "metadata": {
            "lastTriggerTime": crawl_start,  # When crawler was triggered
            "lastCrawlingTime": current_time,  # When crawling finished
            "lastUpdateTime": current_time,  # When data was last updated
            "newBonusesAdded": len(bonuses),  # Total bonuses in this crawl
            "totalValue": total_value,
            "crawlerVersion": "2.0.0"  # ✅ Updated version
        },
        "meta": {
            "disclaimer": "Bonus information is gathered from public sources. Please verify with the financial institution.",
            "crawlerVersion": "2.0.0",  # ✅ Updated version
            "totalValue": total_value
        }
    }

# NOTE: The rest of the parsers would go here (parse_doc_bank, parse_chase, etc.)
# I'm not including them all to keep the file concise, but they should be copied from the original file
# They just need to handle the None returns from parse_common_bonus properly

def main():
    print("🕷️ Starting Bato Crawler (FIXED VERSION)...")
    
    # Scrape all bonuses and get the start timestamp
    bonuses, crawl_start_time = scrape_all_bonuses()
    
    # Format output with metadata
    output = format_output(bonuses, crawl_start_time)
    
    # Write to output directory
    import os
    os.makedirs("output", exist_ok=True)
    with open("output/bonuses.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\n✅ Done. Saved {len(bonuses)} bonuses to output/bonuses.json")
    print(f"💰 Total value: ${output['meta']['totalValue']:,}")
    print(f"📦 Output: output/bonuses.json")

if __name__ == "__main__":
    main()
