import re
from bs4 import BeautifulSoup
from src.models import Bonus

def parse_doctor_credit(html: str) -> list[Bonus]:
    """Parse bank bonuses from Doctor of Credit"""
    bonuses = []
    soup = BeautifulSoup(html, 'lxml')
    for article in soup.select('article'):
        title = article.select_one('.entry-title')
        if not title or 'bank' not in title.text.lower():
            continue
        content = article.select_one('.entry-content')
        # extract offers list
        for li in content.select('li'):
            text = li.get_text()
            # regex to find bank name and bonus amount
            match = re.search(r'([A-Za-z\s]+?):?\s+\$?(\d{1,3}(?:,\d{3})*)', text)
            if match:
                bank = match.group(1).strip()
                amount = float(match.group(2).replace(',', ''))
                # rough difficulty estimate
                difficulty = 0.3 if 'easy' in text.lower() else 0.6
                bonuses.append(Bonus(
                    category='bank',
                    platform=bank,
                    bonus_type='deposit',
                    bonus_amount=amount,
                    currency='USD',
                    bonus_description=text,
                    requirements={},
                    offer_end='',  # may need further parsing
                    difficulty=difficulty,
                    estimated_time_days=90,
                    capital_required=0,
                    country='US',
                    source_url='https://doctorofcredit.com',
                    tags=['aggregated']
                ))
    return bonuses
