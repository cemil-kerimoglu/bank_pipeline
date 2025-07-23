from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg

from utils.data_utils import read_csv_from_s3, write_parquet_to_s3


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
        # load loans and districts
        loan_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "loan.csv"
        )
        dist_df = read_csv_from_s3(
            self.spark, self.bucket, self.in_prefix, "district.csv"
        ).select("district_id", "A2")  # A2 = district name

        # compute average and join to get names
        avg_df = (
            loan_df.groupBy("district_id")
                   .agg(avg("amount").alias("avg_loan_amount"))
                   .join(dist_df, on="district_id", how="left")
        )

        return avg_df

    def save(self, df: DataFrame) -> None:
        write_parquet_to_s3(df, self.bucket, self.out_prefix)
