"""
SparkSession Module
"""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# instantiate common variables; needed when loading packages from library
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
