from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import geohash2


def _geohash(lat, lon, precision: int) -> str:
    if lat is None or lon is None:
        return None
    try:
        return geohash2.encode(float(lat), float(lon), precision=precision)
    except Exception:
        return None


def add_geohash(df: DataFrame, precision: int = 4) -> DataFrame:
    gh_udf = udf(lambda lat, lon: _geohash(lat, lon, precision), StringType())
    return df.withColumn("geohash4", gh_udf(col("latitude"), col("longitude")))
