
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SparkFiles

spark = SparkSession.builder.config("spark.shuffle.service.enabled", True) \
    .enableHiveSupport() \
    .config("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY") \
    .getOrCreate()
print(spark)