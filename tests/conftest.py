import os
import sys
from pathlib import Path

# Ensure the project root directory is in sys.path so that `import src...` works
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pytest
from pyspark.sql import SparkSession

# Ensure Spark uses the current Python interpreter for worker processes (fixes "python3" not found on Windows)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """Provides a local SparkSession that is shared across tests.

    A single-threaded local master is sufficient for unit-level
    transformations while keeping resource usage minimal.
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )
    yield spark
    spark.stop() 