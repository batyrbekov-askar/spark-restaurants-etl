import requests
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType


def geocode_opencage(query: str, api_key: str):
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
    - Detect invalid coords: null or out of range
    - Build a query string from likely columns (address/city/country/name)
    - Distinct queries -> driver REST calls -> join back
    """

    # Собираем query из "типичных" колонок. Если их нет — concat_ws просто пропустит null.
    query = F.concat_ws(
        ", ", F.col("address"), F.col("city"), F.col("country"), F.col("name")
    )

    df = restaurants.withColumn("query", query)

    needs_geocode = (
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < F.lit(-90))
        | (F.col("latitude") > F.lit(90))
        | (F.col("longitude") < F.lit(-180))
        | (F.col("longitude") > F.lit(180))
    )

    df = df.withColumn("needs_geocode", needs_geocode)

    # Берём distinct query только там, где реально нужно
    distinct_queries = (
        df.filter(F.col("needs_geocode") & (F.length(F.col("query")) > 0))
        .select("query")
        .distinct()
    )

    queries = [r["query"] for r in distinct_queries.collect()]

    mapping_rows = []
    for q in queries:
        try:
            lat, lon = geocode_opencage(q, api_key)
        except Exception:
            lat, lon = None, None
        mapping_rows.append((q, lat, lon))

    spark = restaurants.sparkSession
    schema = StructType(
        [
            StructField("query", StringType(), False),
            StructField("lat_fix", DoubleType(), True),
            StructField("lon_fix", DoubleType(), True),
        ]
    )

    fixes_df = spark.createDataFrame(mapping_rows, schema=schema)

    out = (
        df.join(fixes_df, on="query", how="left")
        .withColumn(
            "latitude",
            F.when(
                F.col("needs_geocode") & F.col("lat_fix").isNotNull(), F.col("lat_fix")
            ).otherwise(F.col("latitude")),
        )
        .withColumn(
            "longitude",
            F.when(
                F.col("needs_geocode") & F.col("lon_fix").isNotNull(), F.col("lon_fix")
            ).otherwise(F.col("longitude")),
        )
        .drop("lat_fix", "lon_fix", "needs_geocode")
    )

    return out
