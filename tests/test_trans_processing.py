import pytest
from pyspark.sql import Row

from src.jobs.trans_processing import TransProcessor


@pytest.fixture() # this indicates that this function provides test data
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

    # Build minimal in-memory CSV equivalents as Spark DataFrames
    trans_df = spark.createDataFrame(
        [
            Row(account_id=1, type="PRJIEM"),  # typo will be corrected and the transaction will be kept
            Row(account_id=2, type="VYDAJ"),   # valid type but invalid account ID; should be filtered out
            Row(account_id=3, type="VYDAJ"),   # valid type but invalid account ID; should be filtered out
            Row(account_id=4, type="PRIJEM"),  # valid type and account ID; should be kept
            Row(account_id=5, type="VYDAJ"),   # valid type and account ID; should be kept
        ]
    )
    account_df = spark.createDataFrame([
        Row(account_id=1),
        Row(account_id=4), 
        Row(account_id=5)
    ])

    # Monkey-patch the CSV reader to return our synthetic frames
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

    # Execute and validate
    processor = TransProcessor(spark, trans_processor_config)
    result = processor.run()

    # Validate expected row count
    # Should have 3 transactions (accounts 1, 4, 5) and filter out accounts 2, 3
    assert result.count() == 3
    
    # Check that account 1's typo was corrected
    assert (result.filter((result.account_id == 1) & (result.type == "PRIJEM")).count() == 1)
    
    # Check that accounts 4 and 5 were kept
    assert (result.filter(result.account_id == 4).count() == 1)
    assert (result.filter(result.account_id == 5).count() == 1)
