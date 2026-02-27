from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import hashlib

@dataclass
class Bonus:
    id: str = field(default_factory=str)
    category: str                  # bank, crypto, investment, referral, retail, travel, survey, uk_switch, wealth
    platform: str                   # Chase, Coinbase, Airbnb, etc.
    bonus_type: str                 # signup, referral, deposit, trade, cashback, reward
    bonus_amount: float
    currency: str                   # USD, GBP, EUR, MILES, etc.
    bonus_description: str
    requirements: Dict[str, Any]
    offer_start: Optional[str] = None
    offer_end: str
    difficulty: float                # 0.0-1.0
    estimated_time_days: int
    capital_required: float
    country: str                     # US, UK, Global, etc.
    source_url: str
    terms: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    last_updated: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def generate_id(self) -> str:
        """Create unique ID based on content"""
        content = f"{self.category}-{self.platform}-{self.bonus_amount}-{self.offer_end}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def to_dict(self):
        return asdict(self)
