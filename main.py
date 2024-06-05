import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, split, when, from_utc_timestamp, to_date, dayofmonth, avg, lit, broadcast, lag
from pyspark.sql.window import Window


def create_spark_session():
    return SparkSession.builder \
        .appName("spark") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "Europe/Paris") \
        .getOrCreate()


def define_schema():
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("age_sexe", StringType(), True),
        StructField("application", StringType(), True),
        StructField("time_spent", IntegerType(), True),
        StructField("times_opened", IntegerType(), True),
        StructField("notifications_received", IntegerType(), True),
        StructField("times_opened_after_notification", IntegerType(), True)
    ])


def read_and_union(files, schema, spark):
    final_df = None
    for file_path in files:
        df = spark.read.csv(file_path, schema=schema, header=True)
        final_df = df if final_df is None else final_df.union(df)
    return final_df


def preprocess_data(df):
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    df = df.withColumn("timestamp_paris", from_utc_timestamp(col("timestamp"), "Europe/Paris"))
    df = df.withColumn("age", split(col("age_sexe"), "-").getItem(0).cast("integer"))
    df = df.withColumn("sexe", split(col("age_sexe"), "-").getItem(1))
    df = df.withColumn("sexe", when(col("sexe").isin(["F", "Female"]), "F").otherwise("M"))
    df = df.withColumn("date", to_date(col("timestamp_paris")))
    df = df.withColumn("day", dayofmonth(col("timestamp_paris")))
    return df


def add_age_group(df):
    df = df.withColumn("age_group", when(col("age") < 15, "moins de 15 ans")
                       .when((col("age") >= 15) & (col("age") <= 25), "15-25")
                       .when((col("age") > 25) & (col("age") <= 35), "26-35")
                       .when((col("age") > 35) & (col("age") <= 45), "36-45")
                       .otherwise("ppppplus que 46 ans"))
    return df


def calculate_aggregates(df):
    df_age = df.groupBy("date", "age_group").agg(
        avg("time_spent").alias("value")
    ).withColumn("criterion", lit("âge")).withColumnRenamed("age_group", "variable")

    df_sex = df.groupBy("date", "sexe").agg(
        avg("time_spent").alias("value")
    ).withColumn("criterion", lit("sexe")).withColumnRenamed("sexe", "variable")

    return df_age, df_sex


def join_app_categories(df, app_categories_path, spark):
    app_categories = spark.read.csv(app_categories_path, header=True, inferSchema=True)
    df = df.join(broadcast(app_categories), "application", "left_outer")
    return df


def group_by_category(df):
    return df.groupBy("date", "category").agg(
        avg("time_spent").alias("value")
    ).withColumn("criterion", lit("catégorie")).withColumnRenamed("category", "variable")


def calculate_index(df):
    window_spec = Window.partitionBy("criterion", "variable").orderBy("timestamp")
    df = df.withColumn("index", col("value") - lag("value", 365).over(window_spec))

    window_spec_avg = Window.partitionBy("criterion", "variable").orderBy("timestamp").rowsBetween(-2, 2)
    df = df.withColumn("smoothed_index", avg("index").over(window_spec_avg))

    return df


def save_data(df, output_path):
    os.makedirs(output_path, exist_ok=True)
    try:
        df.repartition(10).write.mode("overwrite").parquet(output_path)
        print(f"Data successfully written to {output_path}")
    except Exception as e:
        print(f"Error writing data: {e}")


def main():
    spark = create_spark_session()

    schema = define_schema()

    input_dir = os.path.join(os.path.dirname(__file__), "data_input")
    files = [os.path.join(input_dir, f"applications_activity_per_user_per_hour_{i}.csv") for i in range(1, 11)]

    df = read_and_union(files, schema, spark)
    df = preprocess_data(df)
    df = add_age_group(df)
    df.cache()

    df_age, df_sex = calculate_aggregates(df)
    df = join_app_categories(df, os.path.join(input_dir, "applications_categories.csv"), spark)
    df_category = group_by_category(df)

    df_final = df_age.unionByName(df_sex).unionByName(df_category)

    df_final = df_final.withColumnRenamed("date", "timestamp")

    df_final = calculate_index(df_final)

    output_path = os.path.join(os.path.dirname(__file__), "data_output")
    save_data(df_final, output_path)

    spark.stop()
    time.sleep(10000)


if __name__ == "__main__":
    main()
