import pytest
from pyspark.sql import Row

from src.jobs.trans_processing import TransProcessor


@pytest.fixture()
def trans_processor_config():
    """Minimal configuration dictionary accepted by TransProcessor."""
    return {
        "s3": {
            "bucket": "dummy-bucket",
            "input_prefix": "",
            "output": {
                "transactions": "processed/cleaned-trans",
                "loans": "processed/avg-loans",
            },
        },
        "cleaning": {
            "trans_type_corrections": {
                "PRJIEM": "PRIJEM",
            }
        },
    }


def test_trans_processor_run(spark, monkeypatch, trans_processor_config):
    """The job should correct typos and drop transactions with unknown account IDs."""

    # ------------------------------------------------------------------
    # Build minimal in-memory CSV equivalents as Spark DataFrames
    # ------------------------------------------------------------------
    trans_df = spark.createDataFrame(
        [
            Row(account_id=1, type="PRJIEM"),  # will be corrected & kept
            Row(account_id=2, type="CREDIT"),  # filtered (missing account)
            Row(account_id=3, type="DEBIT"),   # filtered (missing account)
        ]
    )

    account_df = spark.createDataFrame([Row(account_id=1)])

    # ------------------------------------------------------------------
    # Monkey-patch the CSV reader to return our synthetic frames
    # ------------------------------------------------------------------
    def fake_read_csv_from_s3(_spark, _bucket, _prefix, filename):
        if filename == "trans.csv":
            return trans_df
        elif filename == "account.csv":
            return account_df
        else:
            raise ValueError(f"Unexpected filename: {filename}")

    monkeypatch.setattr(
        "src.jobs.trans_processing.read_csv_from_s3", fake_read_csv_from_s3
    )

    # ------------------------------------------------------------------
    # Execute and validate
    # ------------------------------------------------------------------
    processor = TransProcessor(spark, trans_processor_config)
    result = processor.run()

    # Validate expected row count and content purely via Catalyst filters to
    # avoid materialising rows back to the Python driver (which requires a
    # Python worker process on Windows).
    assert result.count() == 1
    assert (
        result.filter((result.account_id == 1) & (result.type == "PRIJEM")).count()
        == 1
    )
