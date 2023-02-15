from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
                .builder \
                .master("local[3]") \
                .appName("DataSinkDemo") \
                .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    '''
    flightTimeParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()
    '''

    logger.info("Number of partitions before : " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupby(spark_partition_id()).count().show()

    partitionDF = flightTimeParquetDF.repartition(5)
    logger.info("Number of partitions earlier : " + str(partitionDF.rdd.getNumPartitions()))
    partitionDF.groupby(spark_partition_id()).count().show()

    '''
    partitionDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/partitionDF/avro/") \
        .save()
    '''
    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", "20000") \
        .save()
