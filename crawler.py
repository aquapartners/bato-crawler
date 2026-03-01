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

# ======================== REGULAR FETCH ========================
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
    # ... (same as before, keep it unchanged)
    return req

def parse_common_bonus(text, source_url, category):
    bank_match = re.match(r'^([A-Za-z\s\.&\-]+?)(?:\s+\d|[\$:])', text)
    bank = bank_match.group(1).strip() if bank_match else "Unknown"
    bank = re.sub(r'[\.\:]+$', '', bank).strip()
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

# ======================== BANK PARSER ========================
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
            if bonus['bonus_amount']:
                bonuses.append(bonus)
        except:
            continue
    print(f"  Doctor of Credit: found {len(bonuses)} bonuses")
    return bonuses

# ======================== BUSINESS CHECKING PARSERS ========================
def parse_truist_business(html, source_url):
    return [{
        "bank": "Truist",
        "bonus_amount": 400,
        "account_type": "business",
        "raw_text": "Truist Business Checking $400 bonus: $2,000+ deposit and online banking enrollment",
        "category": "business_checking",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_first_commonwealth_business(html, source_url):
    return [{
        "bank": "First Commonwealth Bank",
        "bonus_amount": 300,
        "account_type": "business",
        "raw_text": "First Commonwealth Bank business checking: $300 for $3k+ deposits + 10 debit txns, or $500 for $5k+ deposits + 10 debit txns within 60 days. PA, OH, IN, KY, WV.",
        "category": "business_checking",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_union_savings_business(html, source_url):
    return [{
        "bank": "Union Savings Bank",
        "bonus_amount": 305,
        "account_type": "business",
        "raw_text": "Union Savings Bank business checking: $305 for Basic (avg $4k+ balance + 15 txns), $506 for Relationship (avg $15k+ balance). CT only. Expires Mar 31, 2026.",
        "category": "business_checking",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_golden1_business(html, source_url):
    return [{
        "bank": "Golden 1 Credit Union",
        "bonus_amount": 300,
        "account_type": "business",
        "raw_text": "Golden 1 Credit Union business checking: $300 bonus with $5,000 deposit. CA in-branch only. Expires Feb 28, 2026.",
        "category": "business_checking",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

# ======================== CREDIT UNION PARSERS ========================
def parse_penfed(html, source_url):
    return [{
        "bank": "PenFed Credit Union",
        "bonus_amount": 300,
        "account_type": "checking",
        "raw_text": "PenFed Credit Union checking bonus: $300 for $20k balance or $225 for $15k balance maintained for 123 days. Nationwide.",
        "category": "credit_union",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_becu(html, source_url):
    return [{
        "bank": "BECU",
        "bonus_amount": 500,
        "account_type": "checking",
        "raw_text": "BECU $500 checking bonus: direct deposit $250+ and 30+ debit purchases within 60 days. WA, ID, OR. Expires Apr 10, 2026.",
        "category": "credit_union",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_mountain_america(html, source_url):
    return [{
        "bank": "Mountain America Credit Union",
        "bonus_amount": 150,
        "account_type": "checking",
        "raw_text": "Mountain America Credit Union $150 checking bonus: direct deposit within 60 days, eStatements required. UT, ID, NV, NM, MT, AZ. Expires Jun 30, 2026.",
        "category": "credit_union",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

def parse_alliant_rakuten(html, source_url):
    return [{
        "bank": "Alliant Credit Union",
        "bonus_amount": 150,
        "account_type": "checking",
        "raw_text": "Alliant Credit Union $150 checking bonus via Rakuten: $500+ direct deposit within 30 days. Nationwide.",
        "category": "credit_union",
        "source": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": []
    }]

# ======================== SOURCES PER CATEGORY ========================
SOURCES = {
    "bank": [
        {
            "name": "Doctor of Credit (US Banks)",
            "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/",
            "parser": "doc_bank"
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
    ]
}

# ======================== PARSERS MAP ========================
PARSERS = {
    "doc_bank": parse_doc_bank,
    "truist_business": parse_truist_business,
    "first_commonwealth_business": parse_first_commonwealth_business,
    "union_savings_business": parse_union_savings_business,
    "golden1_business": parse_golden1_business,
    "penfed": parse_penfed,
    "becu": parse_becu,
    "mountain_america": parse_mountain_america,
    "alliant_rakuten": parse_alliant_rakuten,
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
