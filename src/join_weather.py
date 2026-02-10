from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, first


def join_weather_left(restaurants: DataFrame, weather: DataFrame) -> DataFrame:
    """
    Assumes both have 'geohash4'.
    If weather has multiple rows per geohash4, aggregate to avoid multiplication.
    """

    # Пример агрегации: бери средние значения числовых полей (адаптируй под датасет)
    # Допустим weather содержит: temperature, humidity, wind_speed
    # Если у тебя другие поля — поменяй список.
    weather_agg = weather.groupBy("geohash4").agg(
        avg(col("temperature")).alias("temperature_avg"),
        avg(col("humidity")).alias("humidity_avg"),
        avg(col("wind_speed")).alias("wind_speed_avg"),
        first(col("condition"), ignorenulls=True).alias("condition_any"),
    )

    # Left join, рестораны не теряем
    joined = restaurants.join(weather_agg, on="geohash4", how="left")
    return joined
