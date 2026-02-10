from pyspark.sql import DataFrame
from pyspark.sql.functions import col

LAT_CANDIDATES = ["latitude", "lat", "Latitude", "LAT"]
LON_CANDIDATES = ["longitude", "lon", "lng", "long", "Longitude", "LON", "LNG"]


def _find_col(df: DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols_lower:
            return cols_lower[key]
    return None


def normalize_lat_lon(df: DataFrame) -> DataFrame:
    lat_col = _find_col(df, LAT_CANDIDATES)
    lon_col = _find_col(df, LON_CANDIDATES)

    if lat_col and lat_col != "latitude":
        df = df.withColumnRenamed(lat_col, "latitude")
    if lon_col and lon_col != "longitude":
        df = df.withColumnRenamed(lon_col, "longitude")

    return df
