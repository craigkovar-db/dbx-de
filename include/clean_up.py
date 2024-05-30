# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = f"dbfs:/tmp/{user_name}"
fake_data_path = f"{path}/fake_data"

# COMMAND ----------

dbutils.fs.rm(path,True)
