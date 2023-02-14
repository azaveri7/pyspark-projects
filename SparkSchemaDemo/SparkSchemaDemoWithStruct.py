from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimeCsvDf = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")

    flightTimeCsvDf.show()

    logger.info("CSV schema:" + flightTimeCsvDf.schema.simpleString())

    flightTimeJsonDf = spark.read \
        .format("json") \
        .option("header", "true") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
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



