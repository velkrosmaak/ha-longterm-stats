import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
import yaml
from dotenv import load_dotenv

load_dotenv()


class AppConfig(BaseModel):
    ha_url: str = Field(default="http://homeassistant.local:8123")
    ha_token: str = Field(default="")
    db_path: str = Field(default="data/ha_stats.db")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)
    auto_aggregate_interval_minutes: int = Field(default=15)
    prune_raw_days: int = Field(default=0)  # 0 means keep raw data indefinitely
    dormant_days: int = Field(default=14)    # Days without updates to mark entity dormant
    batch_flush_interval: int = Field(default=5)  # Seconds between batch writes
    batch_max_size: int = Field(default=500)      # Max items before flushing queue
    log_level: str = Field(default="INFO")

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "AppConfig":
        config_data = {}
        
        # 1. Try yaml config if exists
        target_path = Path(config_path or "config.yaml")
        if target_path.exists():
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    yaml_content = yaml.safe_load(f)
                    if isinstance(yaml_content, dict):
                        config_data.update(yaml_content)
            except Exception as e:
                print(f"Warning: Failed to load config.yaml: {e}")

        # 2. Environment variables override yaml
        env_mappings = {
            "HA_URL": "ha_url",
            "HA_TOKEN": "ha_token",
            "DB_PATH": "db_path",
            "HOST": "host",
            "PORT": "port",
            "AUTO_AGGREGATE_INTERVAL_MINUTES": "auto_aggregate_interval_minutes",
            "PRUNE_RAW_DAYS": "prune_raw_days",
            "DORMANT_DAYS": "dormant_days",
            "BATCH_FLUSH_INTERVAL": "batch_flush_interval",
            "BATCH_MAX_SIZE": "batch_max_size",
            "LOG_LEVEL": "log_level",
        }
        for env_key, model_key in env_mappings.items():
            val = os.getenv(env_key)
            if val is not None:
                if model_key in ("port", "auto_aggregate_interval_minutes", "prune_raw_days", "dormant_days", "batch_flush_interval", "batch_max_size"):
                    try:
                        config_data[model_key] = int(val)
                    except ValueError:
                        pass
                else:
                    config_data[model_key] = val

        return cls(**config_data)


config = AppConfig.load()
