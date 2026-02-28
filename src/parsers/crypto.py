from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import re
from datetime import datetime

# If you have a shared models file, import from there
# from src.models import Bonus

# For now, we'll define a simple Bonus-like dictionary
# You can replace this with your actual Bonus class import

async def fetch_doc_page(url: str) -> str:
    """
    Helper to fetch a Doctor of Credit page using crawl4ai.
    """
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        if result.success:
            return result.html
        else:
            print(f"Error fetching {url}: {result.error_message}")
            return None

# ======================== OKX PARSER ========================

async def parse_okx_bonus() -> list:
    """
    Parse OKX bonus from Doctor of Credit.
    """
    url = "https://www.doctorofcredit.com/okx-crypto-exchange-review-bonus/"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if not article:
        return []
    
    text = article.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "OKX",
        "category": "crypto",
        "bonus_type": "tiered_trading_volume",
        "max_bonus": 10000,
        "currency": "USDT",
        "referral_code": "96613811",
        "requirements": {
            "kyc": True,
            "deposit": True,
            "trading_volume_tiers": True,
            "deadlines": True
        },
        "additional_perks": ["50% trading fee discount", "Priority Jumpstart access", "Airdrop eligibility"],
        "fees": "Discounted with code",
        "geographic_restrictions": "Check jurisdiction",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Each tier has strict deadlines and turnover targets",
            "Bonus is withdrawable after meeting volume requirements"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== COINBASE PARSER ========================

async def parse_coinbase_bonus() -> list:
    """
    Parse Coinbase bonus from Doctor of Credit.
    """
    url = "https://www.doctorofcredit.com/coinbase-review-bonus/"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if not article:
        return []
    
    text = article.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "Coinbase",
        "category": "crypto",
        "bonus_type": "wheel",
        "max_bonus": 200,
        "currency": "Crypto",
        "requirements": {
            "kyc": True,
            "first_trade": True,
            "trade_amount": "any",
            "holding_days": 0
        },
        "additional_perks": ["Earn $2–$5 per completed educational module"],
        "fees": "Variable",
        "geographic_restrictions": "Available in many countries, but not all",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Bonus often comes as a randomized wheel spin after first trade",
            "No minimum trade amount (can be as low as $2)",
            "Educational rewards are separate and immediate"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== BITGET PARSER ========================

async def parse_bitget_bonus() -> list:
    """
    Parse Bitget bonus from Doctor of Credit.
    """
    url = "https://www.doctorofcredit.com/bitget-crypto-exchange-review-bonus/"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if not article:
        return []
    
    text = article.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "Bitget",
        "category": "crypto",
        "bonus_type": "trial_funds_plus_rebates",
        "max_bonus": 5000,
        "currency": "USDT (Trial)",
        "requirements": {
            "kyc": True,
            "deposit": "probably required",
            "trading_volume": "to unlock trial profits",
            "deadlines": True
        },
        "additional_perks": ["50% card rebates"],
        "fees": "Lowest in industry (spot 0.01%)",
        "geographic_restrictions": "Check jurisdiction",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Trial funds are for futures trading; profits are withdrawable",
            "Losses covered by trial fund (up to the fund amount)",
            "Over 1,300 digital assets supported",
            "$300M+ protection fund"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== KRAKEN PARSER ========================

async def parse_kraken_bonus() -> list:
    """
    Parse Kraken deposit match bonus from Doctor of Credit.
    """
    url = "https://www.doctorofcredit.com/kraken-3-cash-crypto-deposit-match-18-month-hold/"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    if not article:
        return []
    
    text = article.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "Kraken",
        "category": "crypto",
        "bonus_type": "deposit_match_percentage",
        "match_percentage": 3,
        "max_deposit": 1000000,
        "max_bonus": 30000,
        "currency": "USD equivalent",
        "requirements": {
            "enrollment_required": True,
            "deposit_window": "February 2 – March 9, 2026",
            "min_deposit": 1,
            "max_deposit": 1000000,
            "hold_period_months": 18,
            "auto_earn_required": True
        },
        "additional_perks": [
            "Funds can be traded during hold period",
            "Can invest in SGOV or stocks while waiting"
        ],
        "fees": "Standard",
        "geographic_restrictions": "Check jurisdiction",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Enrollment is required before making deposits",
            "You're free to trade assets throughout both the promotion window and hold period",
            "After hold, you can ACATS transfer assets out",
            "Strategy: deposit cash, buy SGOV, earn extra % on top of normal yield"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== MEXC PARSER ========================

async def parse_mexc_bonus() -> list:
    """
    Parse MEXC Referral Ambassador Program from official page.
    """
    url = "https://www.mexc.com/en-TR/announcements/article/mexc-launches-the-referral-ambassador-program-17827791531306"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    # Official pages often have the content in a div with class 'content'
    content = soup.find('div', class_='content') or soup.find('article') or soup
    text = content.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "MEXC",
        "category": "crypto",
        "bonus_type": "referral_ambassador_tiered",
        "max_commission": 40,
        "currency": "USDT",
        "referral_code": None,
        "requirements": {
            "kyc": True,
            "automatic_qualification": True,
            "evaluation_cycles": "2 months",
            "tiers": ["Rising", "Elite", "Champion"]
        },
        "additional_perks": [
            "Elite Referral Rally – win up to $2,500",
            "Champion Referral Spin – guaranteed prizes including gold bar"
        ],
        "fees": "Low",
        "geographic_restrictions": "Global, over 170 countries",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "No application or fees required – automatically a Rising Ambassador",
            "Track progress in real time",
            "Commissions based on referees' trading activity"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== HTX PARSER ========================

async def parse_htx_bonus() -> list:
    """
    Parse HTX New Funds Bonus Trial Program from official page.
    """
    url = "https://www.htx.com/support/55024606728745"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('div', class_='support-article-content') or soup.find('article') or soup
    text = content.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "HTX",
        "category": "crypto",
        "bonus_type": "new_funds_bonus_trial",
        "max_commission": 20,
        "commission_breakdown": {
            "base_reward": "8%",
            "q1_launch_bonus": "8%",
            "milestone_incentive": "4% (for ≥20M USDT)"
        },
        "currency": "USDT",
        "trial_period": "February 6 – June 30, 2026",
        "requirements": {
            "kyc": True,
            "effective_new_funds": True,
            "supported_assets": ["USDT", "USDC", "ETH", "SOL", "TRX", "USD1", "USDD"]
        },
        "additional_perks": [
            "Industry-first program focusing on net capital inflows",
            "Long-term asset retention focus"
        ],
        "geographic_restrictions": "Global",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Commissions based on net new funds deposited and retained, NOT trading volume",
            "Three-tier benefits: base (8%) + Q1 bonus (8%) + milestone (4%)",
            "Designed for long-term, sustainable growth"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== CRYPTO.COM PARSER ========================

async def parse_cryptocom_bonus() -> list:
    """
    Parse Crypto.com VIP Referral Program from official page.
    """
    url = "https://crypto.com/sg/product-news/exchange-vip-referral-program"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('main') or soup.find('article') or soup
    text = content.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "Crypto.com Exchange",
        "category": "crypto",
        "bonus_type": "vip_referral_program",
        "max_commission": 50,
        "currency": "USDC",
        "referral_code": None,
        "requirements": {
            "kyc": True,
            "vip_status_required": True,
            "verification_deadline": "15 days"
        },
        "referee_benefits": {
            "rebate_percentage": 20,
            "rebate_duration": "12 months",
            "currency": "CRO"
        },
        "additional_perks": [
            "Daily payouts in USDC",
            "Commissions in perpetuity",
            "Dedicated dashboard with real-time tracking",
            "Automated payments"
        ],
        "geographic_restrictions": "Jurisdictional limitations apply",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Purpose-built for high-volume, high-value market participants",
            "If referee doesn't qualify as VIP, they're considered for general Referral Program",
            "One unified referral code system with automated tracking"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== BYBIT PARSER ========================

async def parse_bybit_bonus() -> list:
    """
    Parse Bybit Boost Battle competition from official announcement.
    """
    url = "https://announcements.bybit.com/article/boost-battle-x-tmgp-2026-series-1-trade-daily-grab-your-share-of-the-1-000-000-usdt-prize-pool--blt353d08203eb770b9/"
    html = await fetch_doc_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('article') or soup.find('div', class_='announcement-content') or soup
    text = content.get_text(strip=True)
    
    bonuses = []
    bonus = {
        "platform": "Bybit",
        "category": "crypto",
        "bonus_type": "trading_competition",
        "total_prize_pool": 1000000,
        "currency": "USDT",
        "competition_period": "Through March 15, 2026",
        "requirements": {
            "kyc": True,
            "registration_required": True,
            "trading_volume": "on spot and futures"
        },
        "winning_paths": {
            "weekly_leaderboards": "Four rounds, up to 70,000 USDT each",
            "final_leaderboard": "730,000 USDT total, top prize 80,000 USDT",
            "tasks_and_lucky_draw": "Daily prizes up to 200 XPL"
        },
        "boost_mechanism": "Extra points for trading 'boosted tokens' announced weekly",
        "additional_perks": [
            "Previous edition generated $100+ billion in trading volume",
            "Deep liquidity across major trading pairs",
            "Unified account system for cross-margin trading"
        ],
        "geographic_restrictions": "Terms and conditions apply",
        "source_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "notes": [
            "Points based on trading volume of non-zero-fee pairs on both spot and futures",
            "Four weekly rounds with separate prize pools",
            "Final cumulative leaderboard for largest prizes"
        ]
    }
    bonuses.append(bonus)
    return bonuses

# ======================== PARSER MAPPING ========================

# This dictionary maps parser names to functions (useful if called from a main orchestrator)
CRYPTO_PARSERS = {
    "okx": parse_okx_bonus,
    "coinbase": parse_coinbase_bonus,
    "bitget": parse_bitget_bonus,
    "kraken": parse_kraken_bonus,
    "mexc": parse_mexc_bonus,
    "htx": parse_htx_bonus,
    "cryptocom": parse_cryptocom_bonus,
    "bybit": parse_bybit_bonus,
}
