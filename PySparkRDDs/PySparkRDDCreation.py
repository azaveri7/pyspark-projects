from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j

if __name__ == "__main__":
    conf = SparkConf().setAppName("PySparkRDDCreation")

    spark = SparkSession \
        .builder \
        .appName("PySparkRDDCreation") \
        .master("local[2]") \
        .getOrCreate()

    sc = SparkContext.getOrCreate(conf=conf)

    logger = Log4j(spark)

