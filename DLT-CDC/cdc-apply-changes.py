# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def cdc_users():
  return spark.readStream.format("delta").table("craigkovar.craigkovar.cdc_raw")

dlt.create_streaming_table("users")

dlt.apply_changes(
  target = "users",
  source = "cdc_users",
  keys = ["id"],
  sequence_by = col("txn_time"),
  apply_as_deletes = expr("op_type = 'D'"),
  except_column_list = ["op_type", "txn_time"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# To Do - What happens if we change to SCD type 2?  Let's add a new table and compare.

