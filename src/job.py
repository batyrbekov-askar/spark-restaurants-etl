from pyspark.sql import SparkSession
from dotenv import load_dotenv

from config import load_config
from io import read_restaurants, read_weather, write_output
from enrich_geocode import fix_missing_coordinates
from enrich_geohash import add_geohash4
from join_weather import join_weather_left

def build_spark(app_name: str = "restaurants_etl") -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")   # локально в IDE
            .getOrCreate())

def main():
    load_dotenv()
    cfg = load_config()

    if not cfg.opencage_api_key:
        raise RuntimeError("OPENCAGE_API_KEY is empty. Put it into .env")

    spark = build_spark()

    restaurants = read_restaurants(spark, cfg.restaurants_path)
    weather = read_weather(spark, cfg.weather_path)

    restaurants_fixed = fix_missing_coordinates(restaurants, cfg.opencage_api_key)
    restaurants_geo = add_geohash4(restaurants_fixed, precision=cfg.geohash_precision)

    weather_geo = add_geohash4(weather, precision=cfg.geohash_precision)

    enriched = join_weather_left(restaurants_geo, weather_geo)

    write_output(enriched, cfg.output_path)

    spark.stop()

if __name__ == "__main__":
    main()
