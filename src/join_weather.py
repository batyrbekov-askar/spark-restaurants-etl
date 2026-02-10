from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_weather_date(weather: DataFrame) -> DataFrame:
    # из partition columns year/month/day собираем дату
    cols = set(weather.columns)
    if {"year", "month", "day"}.issubset(cols):
        return weather.withColumn(
            "weather_date",
            F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.col("day"))),
        )
    return weather


def dedupe_weather(weather: DataFrame) -> DataFrame:
    """
    Goal: one row per (geohash4, weather_date)
    Strategy: aggregate numeric cols via avg, and non-numeric via first non-null.
    This avoids join multiplication.
    """
    weather = add_weather_date(weather)

    key_cols = ["geohash4"]
    if "weather_date" in weather.columns:
        key_cols.append("weather_date")

    # Подстрой под реальные колонки weather:
    # если у тебя есть temperature/humidity/etc — оставь,
    # если нет — замени на существующие числовые колонки.
    agg_exprs = []
    for c in weather.columns:
        if c in key_cols:
            continue
        # простая эвристика: числа усредняем, остальное берём first
        if dict(weather.dtypes).get(c) in (
            "int",
            "bigint",
            "double",
            "float",
            "smallint",
            "tinyint",
        ):
            agg_exprs.append(F.avg(F.col(c)).alias(f"{c}_avg"))
        else:
            agg_exprs.append(F.first(F.col(c), ignorenulls=True).alias(f"{c}_any"))

    return weather.groupBy(*key_cols).agg(*agg_exprs)


def join_weather_left(restaurants: DataFrame, weather: DataFrame) -> DataFrame:
    weather_1 = dedupe_weather(weather)

    # Если у ресторанов нет даты — join только по geohash4
    if "weather_date" in weather_1.columns and "weather_date" in restaurants.columns:
        return restaurants.join(weather_1, on=["geohash4", "weather_date"], how="left")

    return restaurants.join(weather_1, on=["geohash4"], how="left")
