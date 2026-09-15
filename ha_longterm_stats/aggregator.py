import asyncio
import logging
from ha_longterm_stats.db import Database

logger = logging.getLogger("ha_longterm_stats.aggregator")

class RollupAggregator:
    def __init__(self, db: Database, interval_minutes: int = 15, prune_raw_days: int = 0):
        self.db = db
        self.interval_seconds = interval_minutes * 60
        self.prune_raw_days = prune_raw_days
        self._running = False


    async def start(self):
        self._running = True
        logger.info(f"Starting background rollup aggregator task (runs every {self.interval_seconds // 60} minutes)...")
        while self._running:
            try:
                logger.debug("Executing periodic database rollup calculation...")
                self.db.calculate_rollups()
                if self.prune_raw_days > 0:
                    self.db.prune_raw_data(self.prune_raw_days)
            except Exception as e:
                logger.error(f"Error during rollup calculation: {e}")
            
            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break

    def stop(self):
        self._running = False
