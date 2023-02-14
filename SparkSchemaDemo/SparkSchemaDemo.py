from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeCsvDf = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")

    flightTimeCsvDf.show()

    logger.info("CSV schema:" + flightTimeCsvDf.schema.simpleString())

    flightTimeJsonDf = spark.read \
        .format("json") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("data/flight*.json")

    flightTimeJsonDf.show()

    logger.info("Json schema:" + flightTimeJsonDf.schema.simpleString())

    flightTimeParquetDf = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.parquet")

    flightTimeParquetDf.show()

    logger.info("Parquet schema:" + flightTimeParquetDf.schema.simpleString())



