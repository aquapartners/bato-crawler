# List of sources to crawl
SOURCES = {
    "bank": [
        {"name": "Doctor of Credit", "url": "https://www.doctorofcredit.com/best-bank-account-bonuses/", "type": "aggregator"},
        {"name": "Chase", "url": "https://www.chase.com/personal/checking", "type": "direct"},
        {"name": "Bank of America", "url": "https://www.bankofamerica.com/deposits/checking/", "type": "direct"},
        # ... add 500+ banks
    ],
    "crypto": [
        {"name": "Coinbase", "url": "https://www.coinbase.com/join", "type": "direct"},
        {"name": "Binance", "url": "https://www.binance.com/en/activity", "type": "direct"},
        {"name": "Crypto.com", "url": "https://crypto.com/exchange", "type": "direct"},
        {"name": "OKX", "url": "https://www.okx.com/promotions", "type": "direct"},
        # aggregators like coinmarketcap earn section
    ],
    "investment": [
        {"name": "Robinhood", "url": "https://robinhood.com/", "type": "direct"},
        {"name": "Webull", "url": "https://www.webull.com/activity", "type": "direct"},
        {"name": "TradePMR", "url": "https://www.tradepmr.com/asset-match", "type": "direct"},
    ],
    "referral": [
        {"name": "Airbnb", "url": "https://airbnb.com/invite", "type": "direct"},
        {"name": "Uber", "url": "https://uber.com/invite", "type": "direct"},
        # many referral programs are on partner sites or aggregators
    ],
    "retail": [
        {"name": "Amazon", "url": "https://amazon.com/prime-rewards", "type": "direct"},
        {"name": "Rakuten", "url": "https://rakuten.com/welcome", "type": "direct"},
    ],
    "travel": [
        {"name": "Delta", "url": "https://delta.com/skymiles-offers", "type": "direct"},
        {"name": "Marriott", "url": "https://marriott.com/loyalty", "type": "direct"},
    ],
    "survey": [
        {"name": "Swagbucks", "url": "https://swagbucks.com/offers", "type": "direct"},
        {"name": "Survey Junkie", "url": "https://surveyjunkie.com/", "type": "direct"},
    ],
    "uk_switch": [
        {"name": "MSE", "url": "https://www.moneysavingexpert.com/banking/compare-best-bank-accounts/", "type": "aggregator"},
        {"name": "Nationwide", "url": "https://nationwide.co.uk/switch", "type": "direct"},
    ],
    "wealth": [
        {"name": "Citi Private", "url": "https://privatebank.citibank.com/offers", "type": "direct"},
        {"name": "Morgan Stanley", "url": "https://morganstanley.com/wealth-offers", "type": "direct"},
    ],
}

# Output settings
OUTPUT_FILE = "bonuses.json.encrypted"
DEBUG_FILE = "bonuses_debug.json"
CDN_UPLOAD_URL = "s3://your-bucket/data/"   # optional
