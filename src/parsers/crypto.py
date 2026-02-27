from crawl4ai import AsyncWebCrawler
from src.models import Bonus

async def parse_coinbase() -> list[Bonus]:
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url="https://www.coinbase.com/join")
        # use BeautifulSoup to extract offers from result.html
        # ...
        return bonuses
