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
    """Parse a single bonus line into structured data."""
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

# ======================== ALL PARSER FUNCTIONS (unchanged) ========================
# ... (all your existing parser functions stay exactly as they are) ...
# I'm not repeating the hundreds of lines to save space, but they remain in the final file.

# ======================== SOURCES PER CATEGORY (unchanged) ========================
SOURCES = {
    "bank": [...],
    "business_checking": [...],
    "credit_union": [...],
    "investment": [...],
    "referral": [...],
    "retail": [...],
    "travel": [...],
    "survey": [...],
    "crypto": [...],
    "real_estate": [...]
}

# ======================== PARSERS MAP (unchanged) ========================
PARSERS = {...}

# ======================== FIXED transform_bonus with stopword filtering ========================
def transform_bonus(old_bonus):
    """
    Convert a bonus dict from the old format to the standardized frontend schema.
    Now includes filtering of junk bank names by checking the first word.
    """
    # Validate bonus amount
    bonus_amount = old_bonus.get('bonus_amount')
    if not bonus_amount or bonus_amount <= 0:
        return None
    
    # Validate bank/platform name
    bank_or_platform = old_bonus.get('bank') or old_bonus.get('platform')
    if not bank_or_platform or bank_or_platform.lower() in ['unknown', '', 'none']:
        return None
    
    # NEW: Reject bank names whose first word is a common stopword (junk entries)
    common_stopwords_first_word = {
        'can', 'just', 'there', 'this', 'has', 'was', 'two', 'one', 'direct', 'requires', 'deposit', 
        'bonus', 'offer', 'previously', 'also', 'and', 'the', 'but', 'not', 'so', 'if', 'such', 'as', 
        'at', 'by', 'for', 'from', 'in', 'into', 'of', 'on', 'to', 'with', 'about', 'above', 'across', 
        'after', 'against', 'along', 'among', 'around', 'before', 'behind', 'below', 'beneath', 'beside', 
        'between', 'beyond', 'down', 'during', 'except', 'like', 'near', 'off', 'onto', 'out', 'outside', 
        'over', 'past', 'since', 'through', 'throughout', 'toward', 'under', 'underneath', 'until', 'up', 
        'upon', 'within', 'without', 'be', 'been', 'being', 'is', 'are', 'was', 'were', 'has', 'have', 'had', 
        'do', 'does', 'did', 'may', 'might', 'must', 'shall', 'should', 'will', 'would', 'could', 'that', 
        'these', 'those', 'it', 'they', 'them', 'he', 'she', 'we', 'you', 'his', 'her', 'its', 'our', 'their', 
        'my', 'your', 'no', 'yes'
    }
    first_word = bank_or_platform.split()[0].lower() if bank_or_platform else ''
    if first_word in common_stopwords_first_word:
        return None

    # Validate raw text length
    raw_text = old_bonus.get('raw_text', '')
    if not raw_text or len(raw_text) < 10:
        return None

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

# ======================== scrape_all_bonuses (unchanged) ========================
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
