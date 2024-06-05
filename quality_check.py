import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult



def create_spark_session():
    return SparkSession.builder \
        .appName("Data Quality Check") \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()


def save_data(df, output_path, num_files=10):
    os.makedirs(output_path, exist_ok=True)
    try:
        df = df.repartition(num_files)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Data successfully written to {output_path} in {num_files} files.")
    except Exception as e:
        print(f"Error writing data: {e}")


def main():
    spark = create_spark_session()


    input_path = "/data_output"
    if not os.path.exists(input_path):
        print(f"Input path does not exist: {input_path}")
        return

    df = spark.read.parquet(input_path)

    check = Check(spark, CheckLevel.Error, "example check")
    check = check.hasSize(lambda x: x > 0) \
        .isComplete("timestamp") \
        .isComplete("variable") \
        .isComplete("value") \
        .isComplete("criterion") \
        .isComplete("index") \
        .isComplete("smoothed_index")

    verificationResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    verificationResult_df.show()

    save_data(df, "/data_quality_check", num_files=10)


if __name__ == "__main__":
    main()
