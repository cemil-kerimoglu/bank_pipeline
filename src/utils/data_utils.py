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
    # Build path gracefully whether prefix is empty or not
    if prefix:
        path = f"s3a://{bucket}/{prefix.rstrip('/')}/{filename}"
    else:
        path = f"s3a://{bucket}/{filename}"
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


# -----------------------------------------------------------
# Local filesystem helper
# -----------------------------------------------------------


def write_parquet_local(
    df: DataFrame,
    output_prefix: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame out to a local (project-relative) path in Parquet format.

    The path will be resolved as "data/<output_prefix>", mirroring the layout
    used for raw/processed assets in the repository.
    """
    import os

    # Build path like data/processed/cleaned-trans
    path = os.path.join("data", output_prefix.rstrip("/"))

    # Ensure the parent directory exists so Spark can create the folder tree
    os.makedirs(os.path.dirname(path), exist_ok=True)

    df.write.mode(mode).parquet(path)
