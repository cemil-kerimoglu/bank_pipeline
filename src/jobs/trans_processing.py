from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

from src.utils.data_utils import read_csv_from_s3, write_parquet_local


class TransProcessor:
    """
    Responsible for:
      1) reading raw trans.csv
      2) fixing known typos in the 'type' column
      3) filtering out transactions whose account_id is not present
         in the master account list (acts as our 'contract' table)
      4) writing cleaned transactions to Parquet
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.bucket = config["s3"]["bucket"]
        self.in_prefix = config["s3"]["input_prefix"]
        self.out_prefix = config["s3"]["output"]["transactions"]
        self.typo_map = config["cleaning"]["trans_type_corrections"]

    def run(self) -> DataFrame:
        # 1) load transactions
        trans_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "trans.csv"
        )

        # 2) apply typo corrections
        for wrong, right in self.typo_map.items():
            trans_df = trans_df.withColumn(
                "type",
                when(col("type") == wrong, right).otherwise(col("type"))
            )

        # 3) filter by valid account IDs
        account_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "account.csv"
        ).select("account_id").distinct()

        cleaned = trans_df.join(account_df, on="account_id", how="inner")

        return cleaned

    def save(self, df: DataFrame) -> None:
        """Persist cleaned transactions under data/processed."""
        write_parquet_local(df, self.out_prefix)
