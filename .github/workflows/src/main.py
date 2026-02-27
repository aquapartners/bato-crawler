import asyncio
import logging
from src.config import SOURCES
from src.aggregators import doctor_credit, moneysavingexpert, coinmarketcap
from src.parsers import bank, crypto, investment, referral, retail, travel, survey, uk_switch, wealth
from src.utils.encryption import encrypt_and_save
from src.storage import upload_to_cdn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_crawler():
    all_bonuses = []

    # 1. Run aggregators (they cover many offers at once)
    for agg in [doctor_credit.parse, moneysavingexpert.parse, coinmarketcap.parse]:
        try:
            bonuses = await agg()
            all_bonuses.extend(bonuses)
            logger.info(f"Got {len(bonuses)} from {agg.__name__}")
        except Exception as e:
            logger.error(f"Aggregator failed: {e}")

    # 2. Run direct parsers for each category
    parsers = {
        'bank': bank.parse_all,
        'crypto': crypto.parse_all,
        'investment': investment.parse_all,
        'referral': referral.parse_all,
        'retail': retail.parse_all,
        'travel': travel.parse_all,
        'survey': survey.parse_all,
        'uk_switch': uk_switch.parse_all,
        'wealth': wealth.parse_all,
    }
    for category, parser in parsers.items():
        try:
            bonuses = await parser(SOURCES[category])
            all_bonuses.extend(bonuses)
            logger.info(f"Got {len(bonuses)} from {category}")
        except Exception as e:
            logger.error(f"{category} parsing failed: {e}")

    # 3. Deduplicate (by id)
    unique = {}
    for b in all_bonuses:
        if b.id not in unique:
            unique[b.id] = b
    final = list(unique.values())
    logger.info(f"Total unique bonuses: {len(final)}")

    # 4. Encrypt and save
    encrypt_and_save(final, OUTPUT_FILE)
    # optionally upload to CDN
    upload_to_cdn(OUTPUT_FILE)

if __name__ == "__main__":
    asyncio.run(run_crawler())
