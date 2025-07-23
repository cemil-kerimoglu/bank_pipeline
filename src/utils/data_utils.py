from pyspark.sql import SparkSession, DataFrame


def read_csv_from_s3(
    spark: SparkSession,
    bucket: str,
    prefix: str,
    filename: str,
    sep: str = ";",
    header: bool = True
) -> DataFrame:
    """
    Read a single CSV file from S3 into a Spark DataFrame.
    """
    path = f"s3a://{bucket}/{prefix.rstrip('/')}/{filename}"
    return (
        spark.read
             .option("sep", sep)
             .option("header", header)
             .csv(path, inferSchema=True)
    )


def write_parquet_to_s3(
    df: DataFrame,
    bucket: str,
    output_prefix: str,
    mode: str = "overwrite"
) -> None:
    """
    Write a DataFrame out to S3 in Parquet format.
    """
    path = f"s3a://{bucket}/{output_prefix.rstrip('/')}"
    df.write.mode(mode).parquet(path)
