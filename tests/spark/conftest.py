import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession local e leve para testes automatizados."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("production-data-platform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()
