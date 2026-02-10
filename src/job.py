from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.io import read_restaurants, read_weather, write_output
from normalize import normalize_lat_lon
from enrich_geocode import fix_missing_coordinates
from enrich_geohash import add_geohash
from join_weather import join_weather_left

import os


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("restaurants_weather_etl")
        .master("local[*]")  # IDE local
        .getOrCreate()
    )


def main():
    load_dotenv()

    restaurants_path = os.getenv("RESTAURANTS_PATH", "data/restaurants")
    weather_path = os.getenv("WEATHER_PATH", "data/weather/weather_all")
    output_path = os.getenv("OUTPUT_PATH", "output/enriched_parquet")
    api_key = os.getenv("OPENCAGE_API_KEY", "")

    if not api_key:
        raise RuntimeError("OPENCAGE_API_KEY is empty. Put it into .env")

    spark = build_spark()

    restaurants = read_restaurants(spark, restaurants_path)
    weather = read_weather(spark, weather_path)

    # (1) Нормализуем названия колонок lat/lon (если они lat/lng/...)
    restaurants = normalize_lat_lon(restaurants)
    weather = normalize_lat_lon(weather)

    # (2) Быстрая диагностика (полезно для первого запуска)
    print("Restaurants schema:")
    restaurants.printSchema()
    print("Weather schema:")
    weather.printSchema()

    # (3) Fix missing/invalid lat/lon via OpenCage
    restaurants_fixed = fix_missing_coordinates(restaurants, api_key)

    # (4) Add geohash4
    restaurants_geo = add_geohash(restaurants_fixed, precision=4)
    weather_geo = add_geohash(weather, precision=4)

    # (5) Join (left) without multiplication
    enriched = join_weather_left(restaurants_geo, weather_geo)

    # (6) Write parquet partitions (year/month/day/geohash4 where applicable)
    write_output(enriched, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
