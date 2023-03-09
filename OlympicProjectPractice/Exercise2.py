from pyspark.sql import *
from pyspark.sql.functions import col

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Exercise 1") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    athlete_events_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("data/athlete_events.csv")

    noc_regions_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("data/noc_regions.csv")

    athlete_events_table = athlete_events_df.createOrReplaceTempView("athlete_events")
    noc_regions_table = noc_regions_df.createOrReplaceTempView("noc_regions")

    '''
    Find the Gold medal for athletes over 50 based on sports
    '''

    '''
    First way
    '''
    gold_medal_old_df = spark \
        .sql("SELECT Sport, Age FROM athlete_events WHERE Medal = 'Gold' " \
             "AND Age >= 50 ORDER BY Age DESC")
    gold_medal_old_df.show()
