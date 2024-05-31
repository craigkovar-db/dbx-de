# Databricks notebook source
import dlt
import pandas as pd

from pyspark.sql.functions import sha2, sum
from typing import Iterator
from pyspark.sql.functions import col, pandas_udf

# COMMAND ----------



@pandas_udf("string")
def extract_state_udf(address: pd.Series) -> pd.Series:
    # Use the series.str.extract() method to extract the state from the address string
    # The regular expression '(?P<state>[A-Z]{2})' matches two uppercase letters (i.e., the state abbreviation)
    return address.str.extract('(?P<state>[A-Z]{2})', expand=False)

# COMMAND ----------

@dlt.table
def dlt_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("dbfs:/tmp/craig.kovar@databricks.com/fake_data")
  )

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("email_not_null", "email IS NOT NULL")
def dlt_silver():
  return dlt.read_stream("dlt_bronze").withColumn("state", extract_state_udf("address"))

# COMMAND ----------

@dlt.table
def dlt_gold_masked_table():
  return dlt.read_stream("dlt_silver").withColumn("masked_email",sha2("email", 256)).drop("email")

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise 4 -  Let's create the gold aggregate table using 

# COMMAND ----------

# DBTITLE 1,Exercise 4 - DLT Gold Agg Table
@dlt.table
def dlt_gold_table():
#  return dlt.read(<fill in>).<fill in>
  return dlt.read("dlt_silver").groupBy("state").agg(sum("total_spend").alias("state_spend"))
