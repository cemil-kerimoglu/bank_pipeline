import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, spark_conf_path: str = None) -> SparkSession:
    """
    Build and return a SparkSession configured for S3 access via credentials
    stored in .env file.
    """
    builder = SparkSession.builder.appName(app_name)

    # Pick up existing defaults:
    if spark_conf_path:
        builder = builder.config("spark.defaults.conf", spark_conf_path)

    # Point Spark at the S3A implementation
    builder = builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.sql.parquet.enableVectorizedReader", "true")

    return builder.getOrCreate()
