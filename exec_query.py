#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext, SparkFiles

import sys
import json
import datetime

#reload(sys)
#sys.setdefaultencoding('utf8')


if len(sys.argv) > 1:
    path_hql_file = sys.argv[1]


spark = SparkSession.builder \
    .config("spark.sql.parquet.writeLegacyFormat", True) \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.shuffle.service.enabled", True) \
    .enableHiveSupport() \
    .config("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("hive.exec.dynamic.partition", "true")

spark.sparkContext.addFile('./functions.py')
sys.path.insert(0, SparkFiles.getRootDirectory())

from functions import *

exec_spark_sql(path_hql_file,spark)