from pyspark.sql import SparkSession, DataFrame


def read_restaurants(spark: SparkSession, path: str) -> DataFrame:
    """
    Reads CSV parts from folder like data/restaurants/part-0000...
    If header is missing in dataset, columns will become _c0, _c1, ...
    """
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)  # folder path is OK
    )


def read_weather(spark: SparkSession, path: str) -> DataFrame:
    """
    Reads parquet with partitions: year=YYYY/month=MM/day=DD
    Spark will auto-add partition columns year, month, day.
    """
    return spark.read.parquet(path)


def write_output(df: DataFrame, output_path: str) -> None:
    """
    Idempotent write: overwrite to fixed folder.
    Partitions: year/month/day + geohash4 if available.
    """
    # если в df есть year/month/day — партиционируем так, чтобы сохранить исходную раскладку
    cols = set(df.columns)
    partition_cols = [c for c in ["year", "month", "day", "geohash4"] if c in cols]

    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(output_path)
