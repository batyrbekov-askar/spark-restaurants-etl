from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import functions as F

import requests


def is_invalid_lat_lon(lat, lon) -> bool:
    if lat is None or lon is None:
        return True
    return not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180)


def geocode_opencage(query: str, api_key: str):
    # query: адрес/город/строка из датасета (зависит от полей)
    url = "https://api.opencagedata.com/geocode/v1/json"
    resp = requests.get(
        url,
        params={"q": query, "key": api_key, "limit": 1, "no_annotations": 1},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    if not results:
        return None, None
    geom = results[0].get("geometry", {})
    return geom.get("lat"), geom.get("lng")


def fix_missing_coordinates(restaurants: DataFrame, api_key: str) -> DataFrame:
    """
    Assumes restaurants has columns:
      - latitude, longitude
      - and some location fields to build a query (e.g., address/city/country/name)
    Adapt `query_col` creation to your dataset.
    """

    # Собери строку запроса (пример — подстрой под свои колонки датасета)
    query_col = F.concat_ws(", ", F.col("address"), F.col("city"), F.col("country"))

    # Отберём строки, где координаты плохие
    invalid_df = restaurants.withColumn("query", query_col).withColumn(
        "needs_geocode",
        (col("latitude").isNull())
        | (col("longitude").isNull())
        | (col("latitude") < -90)
        | (col("latitude") > 90)
        | (col("longitude") < -180)
        | (col("longitude") > 180),
    )

    # Чтобы не гонять REST на каждую строку: берём distinct query
    distinct_queries = (
        invalid_df.filter(col("needs_geocode")).select("query").distinct()
    )

    # Собираем distinct query в driver (допустимо для домашки при небольшом объёме)
    queries = [r["query"] for r in distinct_queries.collect()]

    mapping = {}
    for q in queries:
        try:
            lat, lon = geocode_opencage(q, api_key)
        except Exception:
            lat, lon = None, None
        mapping[q] = (lat, lon)

    # Превращаем mapping в DataFrame и join обратно (Spark-friendly)
    spark = restaurants.sparkSession
    schema = StructType(
        [
            StructField("query", F.StringType(), False),
            StructField("lat_fix", DoubleType(), True),
            StructField("lon_fix", DoubleType(), True),
        ]
    )
    rows = [(q, mapping[q][0], mapping[q][1]) for q in mapping]
    fixes_df = spark.createDataFrame(rows, schema=schema)

    joined = (
        invalid_df.join(fixes_df, on="query", how="left")
        .withColumn(
            "latitude",
            when(
                col("needs_geocode") & col("lat_fix").isNotNull(), col("lat_fix")
            ).otherwise(col("latitude")),
        )
        .withColumn(
            "longitude",
            when(
                col("needs_geocode") & col("lon_fix").isNotNull(), col("lon_fix")
            ).otherwise(col("longitude")),
        )
        .drop("lat_fix", "lon_fix", "needs_geocode")
    )

    return joined
