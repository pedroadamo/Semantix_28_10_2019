# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F  
import time

## Read 
df = spark.table("default.access_log_jul95")
df.show()

# COMMAND ----------

## Rename 
oldColumns = df.schema.names
newColumns = ["Host", "x1", "x2","time","x3","Url","erro","bytes"]

df = df.withColumnRenamed(oldColumns[0], newColumns[0]).withColumnRenamed(oldColumns[1], newColumns[1]).withColumnRenamed(oldColumns[2], newColumns[2]).withColumnRenamed(oldColumns[3], newColumns[3]).withColumnRenamed(oldColumns[4], newColumns[4]).withColumnRenamed(oldColumns[5], newColumns[5]).withColumnRenamed(oldColumns[6], newColumns[6]).withColumnRenamed(oldColumns[7], newColumns[7])
df.printSchema()

# COMMAND ----------

#Solução Escalavel
df = df.withColumn("bytes", df["bytes"].cast(IntegerType()))
df.groupBy().agg(F.sum("bytes")).show()

# COMMAND ----------

#Forma não escalavel
df.registerTempTable("df_temp")
df_sql = spark.sql("select sum(bytes) from df_temp")
df_sql.show()
