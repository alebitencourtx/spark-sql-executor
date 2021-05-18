#!/usr/bin/env python3
# -*- codingt: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SparkFiles

import sys
import json
import datetime

#reload(sys)
#sys.setdefaultencoding('utf8')


if len(sys.argv) > 1:
    path_hql_file = sys.argv[1]


if len(sys.argv) > 2:
    CAMINHO = sys.argv[2]

if len(sys.argv) > 3:
    TABLE = sys.argv[3]

spark = SparkSession.builder.config("spark.shuffle.service.enabled", True) \
    .enableHiveSupport() \
    .config("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY") \
    .getOrCreate()



spark.sparkContext.addFile('./functions.py')
sys.path.insert(0, SparkFiles.getRootDirectory())

from functions import *

df = spark.read.format('csv').options(header='true').load(CAMINHO)
df.createOrReplaceTempView(TABLE)
exec_spark_sql(path_hql_file,spark)