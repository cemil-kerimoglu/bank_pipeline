import pytest

from src.utils import spark_utils as su


def test_get_spark_session_raises_without_credentials(monkeypatch):
    """Missing AWS credentials should trigger an EnvironmentError."""

    # Ensure all potential credential env vars are absent
    for var in [
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ACCESS_SECRET",
    ]:
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(EnvironmentError):
        su.get_spark_session("test-app")


def test_get_spark_session_success_with_dummy_builder(monkeypatch):
    """When credentials are present, the builder chain should execute and return our stub session."""

    # Provide minimal AWS env vars so the function proceeds past validation
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    # Stub out the Spark builder so no real JVM / Spark context is started
    class DummyBuilder:
        def __init__(self):
            self.confs = {}

        def appName(self, _name):
            return self

        def config(self, key, value=None):
            # Support both overloaded forms: (key, value) or single string when reading conf file
            if value is None:
                key, value = "", key
            self.confs[key] = value
            return self

        def getOrCreate(self):
            return "dummy-spark-session"

    # Replace the *descriptor* SparkSession.builder with a pre-instantiated dummy
    monkeypatch.setattr(su.SparkSession, "builder", DummyBuilder())

    session = su.get_spark_session("test-app")
    assert session == "dummy-spark-session"
