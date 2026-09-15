import asyncio
import logging
from pathlib import Path
import uvicorn

from ha_longterm_stats.config import config
from ha_longterm_stats.db import Database
from ha_longterm_stats.ha_client import HomeAssistantClient
from ha_longterm_stats.aggregator import RollupAggregator
from ha_longterm_stats.api import app, init_app

logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ha_longterm_stats.main")

async def run_server():
    # 1. Initialize DB
    logger.info(f"Initializing database at {config.db_path}...")
    database = Database(config.db_path)

    # 2. Initialize HA Client
    ha_client = HomeAssistantClient(
        ha_url=config.ha_url,
        token=config.ha_token,
        db=database
    )

    # 3. Initialize Aggregator
    aggregator = RollupAggregator(
        db=database,
        interval_minutes=config.auto_aggregate_interval_minutes,
        prune_raw_days=config.prune_raw_days
    )

    # 4. Initialize FastAPI templates & state
    templates_dir = str(Path(__file__).parent / "ha_longterm_stats" / "templates")
    init_app(database_instance=database, client_instance=ha_client, template_dir=templates_dir)

    # 5. Build Uvicorn config
    uvicorn_config = uvicorn.Config(
        app=app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower()
    )
    server = uvicorn.Server(uvicorn_config)

    # 6. Launch background tasks & server concurrently
    tasks = []
    if config.ha_token:
        logger.info("Home Assistant token provided. Starting live ingestion WebSocket client...")
        tasks.append(asyncio.create_task(ha_client.start()))
    else:
        logger.warning("No HA_TOKEN provided. Ingestion client will not connect until token is configured.")

    tasks.append(asyncio.create_task(aggregator.start()))
    tasks.append(asyncio.create_task(server.serve()))

    logger.info(f"🚀 Home Assistant Long-Term Stats dashboard running at http://{config.host}:{config.port}")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Shutting down Home Assistant Long-Term Stats system...")
    finally:
        await ha_client.stop()
        aggregator.stop()

def main():
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
