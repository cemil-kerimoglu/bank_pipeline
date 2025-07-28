import pytest
from pyspark.sql import Row

from src.jobs.loan_analysis import LoanAnalyzer


@pytest.fixture()
def loan_analyzer_config():
    return {
        "s3": {
            "bucket": "dummy-bucket",
            "input_prefix": "",
            "output": {
                "transactions": "processed/cleaned-trans",
                "loans": "processed/avg-loans",
            },
        }
    }


def test_loan_analyzer_run(spark, monkeypatch, loan_analyzer_config):
    """Average loan amount per district should be computed correctly, ignoring loans
    whose account_id cannot be mapped to a district.
    """

    loan_df = spark.createDataFrame(
        [
            Row(account_id=1, amount=100),
            Row(account_id=2, amount=200),
            Row(account_id=3, amount=300),  # will be dropped (the account cannot be mapped to a district)
            Row(account_id=4, amount=400), 
            Row(account_id=5, amount=500),  
        ]
    )

    account_df = spark.createDataFrame(
        [
            Row(account_id=1, district_id=10),
            Row(account_id=2, district_id=11),
            Row(account_id=4, district_id=10),  
            Row(account_id=5, district_id=11),
            # account 3 is missing, so it will be dropped
        ]
    )

    district_df = spark.createDataFrame(
        [
            Row(district_id=10, A2="District10"),
            Row(district_id=11, A2="District11"),
        ]
    )

    def fake_read_csv_from_s3(_spark, _bucket, _prefix, filename):
        mapping = {
            "loan.csv": loan_df,
            "account.csv": account_df,
            "district.csv": district_df,
        }
        return mapping[filename]

    monkeypatch.setattr(
        "src.jobs.loan_analysis.read_csv_from_s3", fake_read_csv_from_s3
    )

    analyzer = LoanAnalyzer(spark, loan_analyzer_config)
    result = analyzer.run()

    # Cache to avoid recomputation
    result.cache()

    # Assert expected number of rows (one per district)
    assert result.count() == 2

    # Assert each district row exists with the correct average loan amount
    # District 10 should have an average of (100 + 400) / 2 = 250
    # District 11 should have an average of (200 + 500) / 2 = 350

    assert (
        result.filter(
            (result.district_id == 10)
            & (result.A2 == "District10")
            & (result.avg_loan_amount == 250)
        ).count()
        == 1
    )
    assert (
        result.filter(
            (result.district_id == 11)
            & (result.A2 == "District11")
            & (result.avg_loan_amount == 350)
        ).count()
        == 1
    ) 