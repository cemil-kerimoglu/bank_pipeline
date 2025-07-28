import os
from types import SimpleNamespace

from src.utils import data_utils


class DummyCSVReader:
    """Mimics the chained option(...).option(...).csv(...) API."""

    def __init__(self):
        self.received_path = None

    def option(self, *_args, **_kwargs):
        return self  # allows method chaining

    def csv(self, path, inferSchema=True):  # noqa: D401
        self.received_path = path
        # Return something non-None so the caller can proceed
        return path


class DummyWriter:
    def __init__(self):
        self.mode_arg = None
        self.parquet_path = None

    def mode(self, arg):
        self.mode_arg = arg
        return self

    def parquet(self, path):
        self.parquet_path = path


class DummyDataFrame:
    def __init__(self, writer):
        self._writer = writer

    @property
    def write(self):
        return self._writer


def test_read_csv_from_s3_builds_correct_path(monkeypatch):
    dummy_reader = DummyCSVReader()
    dummy_spark = SimpleNamespace(read=dummy_reader)

    result = data_utils.read_csv_from_s3(
        dummy_spark, "mybucket", "my/prefix", "file.csv"
    )

    assert result == "s3a://mybucket/my/prefix/file.csv"
    assert dummy_reader.received_path == "s3a://mybucket/my/prefix/file.csv"


def test_read_csv_from_s3_without_prefix(monkeypatch):
    dummy_reader = DummyCSVReader()
    dummy_spark = SimpleNamespace(read=dummy_reader)

    data_utils.read_csv_from_s3(dummy_spark, "mybucket", "", "file.csv")
    assert dummy_reader.received_path == "s3a://mybucket/file.csv"


def test_write_parquet_local_builds_expected_path(tmp_path, monkeypatch):
    # Use tmp directory as cwd so that the function writes under tmp/data/...
    monkeypatch.chdir(tmp_path)

    dummy_writer = DummyWriter()
    dummy_df = DummyDataFrame(dummy_writer)

    # Patch os.makedirs to avoid touching real FS.
    monkeypatch.setattr(os, "makedirs", lambda *args, **kwargs: None)

    data_utils.write_parquet_local(dummy_df, "processed/cleaned-trans")

    expected_local_path = os.path.join("data", "processed/cleaned-trans")

    assert dummy_writer.mode_arg == "overwrite"
    assert dummy_writer.parquet_path == expected_local_path 