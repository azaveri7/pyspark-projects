from pyspark.sql import *
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

    logger.info("Schema info -> " + athlete_events_df.schema.simpleString())
    logger.info("Schema info -> " + noc_regions_df.schema.simpleString())

    '''
    athlete_events_df.show()
    noc_regions_df.show()
    Schema info -> struct<ID:int,Name:string,Sex:string,
    Age:string,Height:string,Weight:string,Team:string,
    NOC:string,Games:string,Year:string,
    Season:string,City:string,Sport:string,
    Event:string,Medal:string>
    
    struct<NOC:string,region:string,notes:string>
    
    '''

    athlete_events_table = athlete_events_df.createOrReplaceTempView("athlete_events")
    noc_regions_table = noc_regions_df.createOrReplaceTempView("noc_regions")

    '''
    Find the Age distribution from athlete_events for Gold medal athletes
    '''

    '''
    First way
    '''
    gold_medal_df = spark \
        .sql("SELECT Age, COUNT(*) AS Medals FROM athlete_events WHERE Medal = 'Gold' " \
             "GROUP BY Age ORDER BY Age")
    gold_medal_df.show()
    '''
    Second way
    '''
    athlete_events_df.select("Age") \
        .where("Medal = 'Gold'") \
        .groupby("Age") \
        .count() \
        .orderBy("Age") \
        .show()
