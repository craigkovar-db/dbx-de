# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = f"dbfs:/tmp/{user_name}"
fake_data_path = f"{path}/fake_data"

dbutils.widgets.text("catalogformatted", user_name.split('@')[0].replace(".", ""))
dbutils.widgets.text("schemaformatted", user_name.split('@')[0].replace(".", ""))

# COMMAND ----------

print(f"cleaning up {path}")

# COMMAND ----------

dbutils.fs.rm(path,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalogformatted}

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ${schemaformatted} CASCADE;
