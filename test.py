import pytest
from pyspark.sql import SparkSession
from main import create_spark_session, define_schema, preprocess_data, add_age_group, calculate_aggregates

@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session()
    yield spark
    spark.stop()

def test_create_spark_session(spark):
    assert spark is not None
    assert isinstance(spark, SparkSession)

def test_define_schema():
    schema = define_schema()
    assert schema is not None
    assert schema.fieldNames() == ["timestamp", "user_id", "age_sexe", "application", "time_spent", "times_opened", "notifications_received", "times_opened_after_notification"]

def test_preprocess_data(spark):
    schema = define_schema()
    data = [("2020-01-01 00:00:00", 1, "25-M", "App1", 10, 5, 3, 2)]
    df = spark.createDataFrame(data, schema=schema)
    df = preprocess_data(df)
    assert "timestamp_paris" in df.columns
    assert "age" in df.columns
    assert "sexe" in df.columns
    assert "date" in df.columns
    assert "day" in df.columns

def test_add_age_group(spark):
    schema = define_schema()
    data = [("2020-01-01 00:00:00", 1, "25-M", "App1", 10, 5, 3, 2)]
    df = spark.createDataFrame(data, schema=schema)
    df = preprocess_data(df)
    df = add_age_group(df)
    assert "age_group" in df.columns

def test_calculate_aggregates(spark):
    schema = define_schema()
    data = [("2020-01-01 00:00:00", 1, "25-M", "App1", 10, 5, 3, 2)]
    df = spark.createDataFrame(data, schema=schema)
    df = preprocess_data(df)
    df = add_age_group(df)
    df_age, df_sex = calculate_aggregates(df)
    assert df_age is not None
    assert df_sex is not None
