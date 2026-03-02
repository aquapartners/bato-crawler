### 📄 `README.md`

```markdown
# 🕷️ Bato Finance Crawler 

This repository contains the **Python web crawler** that collects bank, crypto, investment, and other financial bonuses for the [Bato Finance](https://bato.market) platform. The crawler runs automatically via GitHub Actions and publishes the results as a static JSON file, which is then consumed by the frontend.

---

## 📦 What's in this repo?

- **`crawler.py`** – The main crawler script (handles all bonus sources).
- **`crypto_parsers.py`** – Crypto‑specific parsing functions.
- **`requirements.txt`** – Python dependencies.
- **`.github/workflows/daily-crawl.yml`** – GitHub Actions workflow (runs every 4 hours).
- **`output/bonuses.json`** – The final output file (generated and committed by the workflow).
- **`README.md`** – This file.

---

## 🚀 How it works

1. **GitHub Actions** triggers the workflow on a schedule (`0 */4 * * *`) or manually.
2. The workflow installs dependencies and runs `crawler.py`.
3. The crawler scrapes all configured sources (banks, crypto exchanges, investment platforms, etc.) and produces a unified `bonuses.json` file in the `output/` directory.
4. If the file has changed, it is committed back to the repository.
5. The frontend (bato‑finance web app) fetches this JSON from a public URL (e.g., raw GitHub URL, GitHub Pages, or a CDN) and displays the bonuses to users.

The result is a **fully automated, zero‑cost data pipeline** that keeps the frontend updated with the latest bonus offers.

---

## 📁 Output format

`bonuses.json` follows this structure:

```json
{
  "bonuses": [
    {
      "id": "chase-300-checking",
      "bonusAmount": 300,
      "category": "bank",
      "capitalRequired": 1000,
      "estimatedTimeDays": 60,
      "difficulty": 2,
      "country": "USA",
      "requirements": "Open account, deposit $1000, receive 1 direct deposit",
      "url": "https://chase.com/checking-bonus",
      "expiryDate": "2026-12-31",
      "tags": ["checking", "direct-deposit"],
      "bank": "Chase",
      "accountType": "checking",
      "scrapedAt": "2026-03-02T12:00:00Z",
      "sourceUrl": "https://chase.com/bonuses",
      "verified": true
    },
    // ... more bonuses
  ],
  "lastUpdated": "2026-03-02T12:00:00Z",
  "bonusCount": 571,
  "version": 1740931200,
  "categories": ["bank", "crypto", "investment", "referral"],
  "sources": ["Chase", "Bank of America", "Coinbase", "Robinhood"],
  "meta": {
    "disclaimer": "Bonus information is gathered from public sources. Please verify with the financial institution.",
    "crawlerVersion": "1.0.0",
    "totalValue": 284500
  }
}
```

The frontend uses the `version` field (a Unix timestamp) to manage caching and only re‑fetch when the data changes.

---

## 🔧 For Developers

### Running locally

1. Clone the repository:
   ```bash
   git clone https://github.com/aquapartners/bato-crawler.git
   cd bato-crawler
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   pip install -r requirements.txt
   ```

3. Install Playwright browsers (if you plan to scrape dynamic sites):
   ```bash
   playwright install chromium
   ```

4. Run the crawler:
   ```bash
   python crawler.py
   ```

5. Check the generated `output/bonuses.json`.

### Adding or modifying sources

- All source definitions are in the `SOURCES` dictionary inside `crawler.py`.
- Each source has a `parser` key that maps to a function in the `PARSERS` dictionary.
- To add a new source, create a parser function and add an entry to both `SOURCES` and `PARSERS`.

### Testing the workflow

- Push changes to the `main` branch; the GitHub Action will run automatically.
- You can also trigger it manually from the Actions tab.

---

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
```

---

### Summary of Changes

- **Removed** references to the decentralized P2P network, DHT, version.txt, and user‑device scraping.
- **Added** a clear explanation of the GitHub Actions + Python crawler pipeline.
- **Described** the output JSON format in detail.
- **Included** local development instructions.
- **Updated** license section.
