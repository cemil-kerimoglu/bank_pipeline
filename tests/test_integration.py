import pytest
from pyspark.sql import Row

from src.jobs.trans_processing import TransProcessor
from src.jobs.loan_analysis import LoanAnalyzer


@pytest.mark.integration
def test_trans_processor_and_loan_analyzer_interoperate(spark, monkeypatch):
    """Integration test of the two Spark jobs without invoking the CLI.

    We still stub external I/O (S3 + parquet writes) but run both jobs back-to-back
    to ensure they function correctly together using the same SparkSession and
    shared configuration.
    """

    # ------------------------------------------------------------------
    # Synthetic data — a superset that satisfies both jobs
    # ------------------------------------------------------------------
    trans_df = spark.createDataFrame([
        Row(account_id=1, type="PRJIEM"),
        Row(account_id=2, type="VYDAJ"),   
        Row(account_id=3, type="VYDAJ"),   # will be filtered 
        Row(account_id=4, type="PRIJEM"),
        Row(account_id=5, type="VYDAJ"),
    ])

    loan_df = spark.createDataFrame([
        Row(account_id=1, amount=100),
        Row(account_id=2, amount=200),
        Row(account_id=3, amount=300),  # unknown district (dropped)
        Row(account_id=4, amount=400),
        Row(account_id=5, amount=500),
    ])

    account_df = spark.createDataFrame([
        Row(account_id=1, district_id=10),
        Row(account_id=2, district_id=11),
        Row(account_id=4, district_id=10),
        Row(account_id=5, district_id=11),
        # account 3 intentionally missing
    ])

    district_df = spark.createDataFrame([
        Row(district_id=10, A2="District10"),
        Row(district_id=11, A2="District11"),
    ])

    # ------------------------------------------------------------------
    # Monkey-patch CSV reader for both jobs
    # ------------------------------------------------------------------

    def fake_read_csv_from_s3(_spark, _bucket, _prefix, filename):
        mapping = {
            "trans.csv": trans_df,
            "loan.csv": loan_df,
            "account.csv": account_df,
            "district.csv": district_df,
        }
        return mapping[filename]

    monkeypatch.setattr("src.jobs.trans_processing.read_csv_from_s3", fake_read_csv_from_s3)
    monkeypatch.setattr("src.jobs.loan_analysis.read_csv_from_s3", fake_read_csv_from_s3)

    # Capture parquet writes (optional)
    monkeypatch.setattr("src.jobs.trans_processing.write_parquet_local", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("src.jobs.loan_analysis.write_parquet_local", lambda *_args, **_kwargs: None)

    # ------------------------------------------------------------------
    # Shared config and execution
    # ------------------------------------------------------------------
    config = {
        "s3": {
            "bucket": "dummy-bucket",
            "input_prefix": "",
            "output": {
                "transactions": "processed/cleaned-trans",
                "loans": "processed/avg-loans",
            },
        },
        "cleaning": {
            "trans_type_corrections": {"PRJIEM": "PRIJEM"},
        },
    }

    tp = TransProcessor(spark, config)
    cleaned_trans = tp.run().cache()

    la = LoanAnalyzer(spark, config)
    avg_loans = la.run().cache()

    # ------------------------------------------------------------------
    # Assertions spanning both jobs
    # ------------------------------------------------------------------
    assert cleaned_trans.count() == 4  # accounts 1,2,4,5 (3 filtered out)
    assert cleaned_trans.filter((cleaned_trans.account_id == 1) & (cleaned_trans.type == "PRIJEM")).count() == 1
    # Ensure account 2 is retained
    assert cleaned_trans.filter(cleaned_trans.account_id == 2).count() == 1

    assert avg_loans.count() == 2  # one row per district
    assert avg_loans.filter((avg_loans.district_id == 10) & (avg_loans.avg_loan_amount == 250)).count() == 1
    assert avg_loans.filter((avg_loans.district_id == 11) & (avg_loans.avg_loan_amount == 350)).count() == 1
    # Any loan without a valid district_id should be removed
    assert avg_loans.filter(avg_loans.district_id.isNull()).count() == 0 