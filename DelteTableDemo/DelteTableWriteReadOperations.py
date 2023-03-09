from pyspark.sql import *
from delta import *
from pyspark.sql.functions import *

from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")
    survey_df.show()

    filtered_df = survey_df.filter((survey_df["Country"] == "Canada") \
                                   | (survey_df["Country"] == "United States"))

    aggregate_df = filtered_df.groupby("Country").agg(count("*"))
    aggregate_df.show()


    filtered_df \
        .coalesce(2) \
        .write \
        .mode("overwrite") \
        .format("delta") \
        .save("dataOutput/delta")

    read_df = spark.read \
                .format("delta") \
                .load("dataOutput/delta")

    read_df.show()



