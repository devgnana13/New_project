import logging
from datetime import datetime, timedelta
import random
from core.database import DatabaseManager
from core.instrument_manager import STOCK_SYMBOLS
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

def seed_db():
    db = DatabaseManager()
    if not db.connect():
        logging.error("Failed to connect to MongoDB")
        return

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Let's seed some realistic scale numbers for Indian indices/stocks since options volume is huge
    volumes = {}
    for sym in STOCK_SYMBOLS:
        # Generate some massive random volume
        # Make random chance to have very low previous volume to trigger 2x spikes
        is_spiking = random.random() < 0.15
        
        base_vol = random.randint(1000000, 50000000)
        
        call_vol = base_vol * (0.4 if is_spiking else random.uniform(1, 3))
        put_vol = base_vol * (0.3 if is_spiking else random.uniform(1, 3))
        
        volumes[sym] = {
            "call_volume": int(call_vol),
            "put_volume": int(put_vol)
        }

    db.store_eod_volumes(volumes, date=yesterday)
    logging.info(f"Successfully seeded {len(volumes)} symbols into DB for date: {yesterday}")

if __name__ == "__main__":
    seed_db()
