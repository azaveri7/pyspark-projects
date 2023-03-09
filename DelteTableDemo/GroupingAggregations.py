from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("DelteTableDemo Info:") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")
    survey_df.show()

    filtered_df = survey_df.filter((survey_df["Country"] == "Canada") \
                                   & (survey_df["family_history"] == "Yes"))
    logger.info(filtered_df.count())

    aggregate_df = survey_df.groupby("Country").agg(avg("Age"))
    aggregate_df.show()

    ordered_agg_df = aggregate_df.orderBy(aggregate_df["avg(Age)"].desc())
    ordered_agg_df.show()

    aggregate_filtered_df = filtered_df.groupby("Country").agg(avg("Age"))
    aggregate_filtered_df.show()
