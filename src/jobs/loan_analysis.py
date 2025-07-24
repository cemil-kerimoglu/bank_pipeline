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

        # Output locations

        # 1) S3 ‑ keep the original prefix so this job can still write to
        #    Amazon S3 when the pipeline is executed in the cloud. We do not
        #    use it in the current local-only setup, but we keep the value for
        #    completeness / future deployments.
        self.s3_out_prefix = config["s3"]["output"]["loans"]

        # 2) Local filesystem ‑ for local development we write results under
        #    data/processed/avg-loans. This single string can be adjusted if the
        #    output is desired somewhere else on the machine.
        self.local_out_prefix = "processed/avg-loans"

    def run(self) -> DataFrame:
        # 1) load the tables (i.e., csv files)

        # loans: contains account_id but no district_id
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
        
        # 2) enrich loans with district_id via accounts

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
        """Persist aggregated loan data.

        By default we write to the local filesystem so that the Parquet files
        end up in the repository’s data folder (e.g. data/processed/avg-loans).

        If the output needs to be stored directly to S3, simply uncomment
        the second call below and ensure valid AWS credentials are available.
        """

        # ---- Local write (default for development) ----
        write_parquet_local(df, self.local_out_prefix)

        # ---- S3 write (optional) ----
        # from src.utils.data_utils import write_parquet_to_s3  # noqa: F401
        # write_parquet_to_s3(df, self.bucket, self.s3_out_prefix)
