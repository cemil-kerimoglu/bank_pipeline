import pytest
from pyspark.sql import Row

import main


# Mark as end-to-end (system) test
@pytest.mark.e2e
# The e2e mark allows users to run "-m e2e" explicitly; CI/CD pipeline runs it in a dedicated job.
# The test exercises the full end-to-end data flow: reading raw CSVs, transforming them
# via both TransProcessor and LoanAnalyzer, and finally writing Parquet outputs. All
# external I/O (S3 & local FS) is stubbed out so the test remains fast and hermetic.

def test_end_to_end_pipeline(spark, monkeypatch):
    """Run the main pipeline end-to-end with in-memory dataframes.

    The test verifies that:
    1. Transactions are cleaned & filtered correctly by TransProcessor.
    2. Average loan amounts per district are computed correctly by LoanAnalyzer.
    3. Both jobs invoke their respective save() methods with the expected output prefixes.
    """

    # ------------------------------------------------------------------
    # 1) Prepare synthetic source data frames
    # ------------------------------------------------------------------
    trans_df = spark.createDataFrame(
        [
            Row(account_id=1, type="PRJIEM"),  # typo that should be corrected
            Row(account_id=2, type="VYDAJ"),   # filtered later – account_id missing in cleaned set
            Row(account_id=3, type="VYDAJ"),   # filtered later – account_id missing in cleaned set
            Row(account_id=4, type="PRIJEM"),  # valid
            Row(account_id=5, type="VYDAJ"),   # valid
        ]
    )

    account_df = spark.createDataFrame(
        [
            Row(account_id=1, district_id=10),
            Row(account_id=2, district_id=11),
            Row(account_id=4, district_id=10),
            Row(account_id=5, district_id=11),
            # NOTE: account_id 3 intentionally missing
        ]
    )

    loan_df = spark.createDataFrame(
        [
            Row(account_id=1, amount=100),
            Row(account_id=2, amount=200),
            Row(account_id=3, amount=300),  # dropped later – unknown district
            Row(account_id=4, amount=400),
            Row(account_id=5, amount=500),
        ]
    )

    district_df = spark.createDataFrame(
        [
            Row(district_id=10, A2="District10"),
            Row(district_id=11, A2="District11"),
        ]
    )

    # ------------------------------------------------------------------
    # 2) Stub out external I/O helpers to operate purely in-memory
    # ------------------------------------------------------------------

    def fake_read_csv_from_s3(_spark, _bucket, _prefix, filename):
        """Return the corresponding in-memory DataFrame for each CSV file name."""
        mapping = {
            "trans.csv": trans_df,
            "account.csv": account_df,
            "loan.csv": loan_df,
            "district.csv": district_df,
        }
        try:
            return mapping[filename]
        except KeyError as exc:
            raise ValueError(f"Unexpected filename requested: {filename}") from exc

    # Patch the function reference inside each job module (they imported it directly)
    monkeypatch.setattr("src.jobs.trans_processing.read_csv_from_s3", fake_read_csv_from_s3)
    monkeypatch.setattr("src.jobs.loan_analysis.read_csv_from_s3", fake_read_csv_from_s3)

    # Capture Parquet writes instead of hitting the local filesystem
    captured_writes = {}

    def capture_write_parquet_local(df, output_prefix, mode="overwrite"):
        # Cache the DF so we can run actions after the pipeline completes
        captured_writes[output_prefix] = df.cache()

    monkeypatch.setattr("src.jobs.trans_processing.write_parquet_local", capture_write_parquet_local)
    monkeypatch.setattr("src.jobs.loan_analysis.write_parquet_local", capture_write_parquet_local)

    # Re-use the session provided by the pytest fixture instead of creating a new one.
    monkeypatch.setattr(main, "get_spark_session", lambda *_args, **_kwargs: spark)

    # Prevent the pipeline from stopping the shared SparkSession so other tests can reuse it.
    monkeypatch.setattr(spark, "stop", lambda: None)

    # ------------------------------------------------------------------
    # 3) Execute the full pipeline (main.main())
    # ------------------------------------------------------------------
    main.main()

    # ------------------------------------------------------------------
    # 4) Assertions – make sure everything worked end-to-end
    # ------------------------------------------------------------------
    assert set(captured_writes.keys()) == {
        "processed/cleaned-trans",
        "processed/avg-loans",
    }

    cleaned_df = captured_writes["processed/cleaned-trans"]
    avg_df = captured_writes["processed/avg-loans"]

    # Cleaned transactions: only accounts 1, 4, 5 should remain & typo corrected
    assert cleaned_df.count() == 3
    assert cleaned_df.filter(
        (cleaned_df.account_id == 1) & (cleaned_df.type == "PRIJEM")
    ).count() == 1

    # Average loans: expect 2 districts with the correct aggregated amounts
    assert avg_df.count() == 2

    # District 10 average = (100 + 400) / 2
    assert (
        avg_df.filter((avg_df.district_id == 10) & (avg_df.avg_loan_amount == 250)).count()
        == 1
    )

    # District 11 average = (200 + 500) / 2
    assert (
        avg_df.filter((avg_df.district_id == 11) & (avg_df.avg_loan_amount == 350)).count()
        == 1
    ) 