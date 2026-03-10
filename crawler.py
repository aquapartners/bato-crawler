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
DELAY = 2  # seconds between requests to the same domain
MAX_PAGES_PER_DOMAIN = 100
MAX_EXTERNAL_LINKS_PER_PAGE = 5  # limit external links to avoid explosion
MAX_NEW_DOMAINS_PER_RUN = 50     # cap new domains added per run
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# Static seeds – you can still add new ones manually
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

# File to store discovered domains
DISCOVERED_DOMAINS_FILE = "discovered_domains.json"

# ======================== ROBOTS.TXT CACHE ========================
robots_parsers: Dict[str, robotexclusionrulesparser.RobotExclusionRulesParser] = {}

async def can_fetch(url: str, user_agent: str = USER_AGENT) -> bool:
    """Check robots.txt for the given URL."""
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
                        rp.parse("")  # empty = allow everything
        except Exception:
            rp.parse("")  # on error, allow
        robots_parsers[domain] = rp
    return robots_parsers[domain].is_allowed(user_agent, url)

# ======================== PERSISTENT SEEDS ========================
def load_all_seeds() -> List[str]:
    """Load static seeds and previously discovered domains."""
    seeds = set(STATIC_SEED_URLS)
    if os.path.exists(DISCOVERED_DOMAINS_FILE):
        try:
            with open(DISCOVERED_DOMAINS_FILE, 'r') as f:
                discovered = json.load(f)
                # discovered can be a list of domain strings or full URLs
                for item in discovered:
                    # if it's a full URL, use it; otherwise construct https://domain
                    if item.startswith('http'):
                        seeds.add(item)
                    else:
                        seeds.add(f"https://{item}")
        except Exception as e:
            print(f"⚠️ Could not load discovered domains: {e}")
    return list(seeds)

def save_new_domains(new_domains: Set[str]) -> None:
    """Append newly discovered domains to the persistent file."""
    existing = set()
    if os.path.exists(DISCOVERED_DOMAINS_FILE):
        try:
            with open(DISCOVERED_DOMAINS_FILE, 'r') as f:
                existing = set(json.load(f))
        except:
            pass
    # Merge and limit to MAX_NEW_DOMAINS_PER_RUN (we'll add at most that many)
    all_domains = existing.union(new_domains)
    with open(DISCOVERED_DOMAINS_FILE, 'w') as f:
        json.dump(list(all_domains), f, indent=2)

# ======================== FETCH HELPERS ========================
async def fetch_url_async(url: str) -> Optional[str]:
    """Fetch a static page using aiohttp (async)."""
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
    """Fetch a dynamic page using crawl4ai (async)."""
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

# ======================== ORIGINAL EXTRACTION HELPERS (unchanged) ========================
# ... (keep all your existing helper functions: extract_amount, extract_requirements,
#      parse_common_bonus, all custom parsers, extract_with_selectors, heuristic_extract_bonus,
#      load_sources, CUSTOM_PARSERS, KNOWN_BANKS, BANK_SUFFIXES, COMMON_STOPWORDS_FIRST_WORD,
#      transform_bonus, format_output) – they are all the same as in your last code.
# For brevity, I'm not repeating them here, but they must be included in the final file.
# The functions from lines 1–1380 in your provided code remain exactly as they are.
# I'll assume they are present in the final version.

# ======================== DISCOVERY CRAWLER (ENHANCED) ========================
async def discover_incentive_pages(
    seed_url: str,
    max_pages: int = MAX_PAGES_PER_DOMAIN,
    follow_external: bool = True,          # allow external links to find new domains
    delay: float = DELAY
) -> Tuple[List[str], Set[str]]:
    """
    Crawl from seed_url, collect pages that contain incentive keywords.
    If follow_external is True, also follow a limited number of external links
    and record new domains that host incentive pages.
    Returns (candidate_urls, newly_discovered_domains).
    """
    visited_pages: Set[str] = set()
    candidates: List[str] = []
    new_domains: Set[str] = set()
    queue: List[str] = [seed_url]
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

                # Extract links
                links = await page.eval_on_selector_all('a[href]', 'els => els.map(e => e.href)')
                external_count = 0
                for link in links:
                    full_url = urljoin(url, link)
                    parsed = urlparse(full_url)
                    # Internal link (same domain)
                    if parsed.netloc == seed_domain:
                        if full_url not in visited_pages and full_url not in queue:
                            queue.append(full_url)
                    # External link – only if follow_external and we haven't exceeded limit
                    elif follow_external and external_count < MAX_EXTERNAL_LINKS_PER_PAGE:
                        external_count += 1
                        # We need to quickly check if this external domain might have incentives
                        # Instead of crawling deeply, we'll just check the homepage later.
                        # For now, record the domain.
                        new_domains.add(parsed.netloc)
                        # Optionally, we could queue the homepage for immediate crawling,
                        # but that could explode quickly. We'll just save the domain.

                await asyncio.sleep(delay + random.uniform(0, 1))

            except Exception as e:
                print(f"    ❌ Error: {e}")

        await browser.close()
    return candidates, new_domains

# ======================== MAIN ORCHESTRATOR ========================
async def run_autonomous_crawler():
    print("🕷️ Starting autonomous global bonus crawler...")
    crawl_start_time = int(datetime.utcnow().timestamp() * 1000)

    # Load all seeds (static + previously discovered)
    all_seeds = load_all_seeds()
    print(f"🌱 Loaded {len(all_seeds)} seed URLs (static + discovered).")

    # Step 1: Discover candidate URLs and new domains from each seed
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

    # Step 2: Validate new domains – we need to check if they actually contain incentive pages.
    # We'll do a quick crawl of the homepage (or a single page) for each new domain.
    validated_new_domains = set()
    for domain in all_new_domains:
        # Construct a homepage URL
        homepage = f"https://{domain}"
        if not await can_fetch(homepage):
            print(f"⚠️ {homepage} disallowed by robots.txt, skipping.")
            continue
        print(f"🔍 Validating new domain: {homepage}")
        # Fetch the homepage using our async fetcher (static or dynamic as needed)
        html = await fetch_url_async(homepage)
        if html is None:
            html = await fetch_dynamic_async(homepage)
        if html is None:
            continue
        # Check for incentive keywords
        if any(kw in html.lower() for kw in INCENTIVE_KEYWORDS):
            validated_new_domains.add(domain)
            print(f"  ✅ Validated (contains incentives)")
        else:
            print(f"  ❌ No incentives found")

    print(f"✅ Validated new domains: {len(validated_new_domains)}")

    # Step 3: Save validated new domains to persistent storage (limit to MAX_NEW_DOMAINS_PER_RUN)
    if len(validated_new_domains) > MAX_NEW_DOMAINS_PER_RUN:
        # take a random sample to stay within limit
        import random
        validated_new_domains = set(random.sample(list(validated_new_domains), MAX_NEW_DOMAINS_PER_RUN))
    save_new_domains(validated_new_domains)

    # Step 4: Load known custom parsers
    known_parsers = CUSTOM_PARSERS

    # Step 5: Process each discovered URL (and also check sources.json)
    raw_bonuses = []
    # Combine discovered URLs and known sources from sources.json
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

    # Step 6: Deduplicate and transform
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

    # Step 7: Format output
    output = format_output(transformed, crawl_start_time)

    os.makedirs("output", exist_ok=True)
    with open("output/bonuses.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Done. Saved {len(transformed)} bonuses to output/bonuses.json")
    print(f"📦 Output: output/bonuses.json")
    print(f"🌱 New domains added to seed list: {len(validated_new_domains)}")

if __name__ == "__main__":
    asyncio.run(run_autonomous_crawler())
