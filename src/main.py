import os
from dotenv import load_dotenv
import yaml

from utils.spark_utils import get_spark_session
from jobs.trans_processing import TransProcessor
from jobs.loan_analysis    import LoanAnalyzer


def main():
    # 1) load .env and pipeline config
    load_dotenv()  # picks up AWS_*, etc.
    with open("conf/pipeline.yaml", "r") as f:
        config = yaml.safe_load(f)

    # 2) start Spark
    spark = get_spark_session("BankDataPipeline", spark_conf_path="conf/spark-defaults.conf")

    # 3) run & save transactions job
    trans_job = TransProcessor(spark, config)
    cleaned_trans = trans_job.run()
    trans_job.save(cleaned_trans)

    # 4) run & save loan analysis job
    loan_job = LoanAnalyzer(spark, config)
    avg_loans = loan_job.run()
    loan_job.save(avg_loans)

    spark.stop()


if __name__ == "__main__":
    main()
