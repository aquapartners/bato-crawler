import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

def scrape_doctor_credit():
    url = "https://www.doctorofcredit.com/best-bank-account-bonuses/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    bonuses = []
    for article in soup.select('article'):
        title = article.select_one('.entry-title')
        if title and 'bank' in title.text.lower():
            for li in article.select('li'):
                text = li.get_text()
                if '$' in text:
                    bonuses.append({
                        'description': text,
                        'source': url,
                        'scraped_at': datetime.now().isoformat()
                    })
    return bonuses

def save_json(data):
    with open('bonuses.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    print("Scraping...")
    bonuses = scrape_doctor_credit()
    save_json(bonuses)
    print(f"Saved {len(bonuses)} bonuses.")
