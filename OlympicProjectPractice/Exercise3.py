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
    Find the number of women medals per edition (summer season) of the Games
    '''

    '''
    First way
    '''
    medal_female_df = spark \
        .sql("SELECT COUNT(Medal) AS Medals, Year FROM athlete_events WHERE Sex = 'F' AND " \
             " Season = 'Summer' AND "
             " Medal IN ('Gold','Silver','Bronze') GROUP BY Year ORDER BY Year")
    medal_female_df.show()
