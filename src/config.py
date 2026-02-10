from dataclasses import dataclass
import os

@dataclass(frozen=True)
class JobConfig:
    restaurants_path: str
    weather_path: str
    output_path: str
    opencage_api_key: str
    geohash_precision: int = 4

def load_config() -> JobConfig:
    return JobConfig(
        restaurants_path=os.getenv("RESTAURANTS_PATH", "data/restaurants.csv"),
        weather_path=os.getenv("WEATHER_PATH", "data/weather.csv"),
        output_path=os.getenv("OUTPUT_PATH", "output/enriched_parquet"),
        opencage_api_key=os.getenv("OPENCAGE_API_KEY", ""),
    )
