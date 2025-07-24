from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg

from src.utils.data_utils import read_csv_from_s3, write_parquet_local


class LoanAnalyzer:
    """
    Reads loan.csv and district.csv, computes the average loan amount per district,
    and writes the result out as Parquet.
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.bucket = config["s3"]["bucket"]
        self.in_prefix = config["s3"]["input_prefix"]
        self.out_prefix = config["s3"]["output"]["loans"]

    def run(self) -> DataFrame:
        # ---------------------------------------------------------
        # 1) load base tables
        # ---------------------------------------------------------

        # loans: contains account_id but _no_ district_id
        loan_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "loan.csv"
        )

        # account: maps account_id -> district_id
        account_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "account.csv"
        ).select("account_id", "district_id")

        dist_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "district.csv"
        ).select("district_id", "A2")  # A2 = district name

        # ---------------------------------------------------------
        # 2) enrich loans with district_id via accounts
        # ---------------------------------------------------------

        loan_enriched = loan_df.join(account_df, on="account_id", how="left")

        # sanity filter: drop rows where district_id is null (no account match)
        loan_enriched = loan_enriched.filter(loan_enriched.district_id.isNotNull())

        # 3) compute average amount per district and attach district names
        avg_df = (
            loan_enriched.groupBy("district_id")
                        .agg(avg("amount").alias("avg_loan_amount"))
                        .join(dist_df, on="district_id", how="left")
        )

        return avg_df

    def save(self, df: DataFrame) -> None:
        """Persist aggregated loan data under data/processed."""
        write_parquet_local(df, self.out_prefix)
