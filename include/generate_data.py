# Databricks notebook source
# MAGIC %pip install Faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = f"dbfs:/tmp/{user_name}"
fake_data_path = f"{path}/fake_data"
schema_path = f"{path}/schema"
checkpoint_path = f"{path}/checkpoint"
al_checkpoint_path = f"{path}/al_checkpoint"


# COMMAND ----------

dbutils.fs.mkdirs(path)
dbutils.fs.mkdirs(fake_data_path)
dbutils.fs.mkdirs(schema_path)
dbutils.fs.mkdirs(checkpoint_path)

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
from faker import Faker
import time
import random

fake = Faker()
schema = T.StructType([
  T.StructField("name", T.StringType()),
  T.StructField("address", T.StringType()),
  T.StructField("phone", T.StringType()),
  T.StructField("email", T.StringType()),
  T.StructField("total_spend", T.LongType())
])

def _gen_row():
  if random.randint(0,100) >= 97:
    return (fake.name(), fake.address(), fake.phone_number(), None, fake.random_int(min=100, max=100000))
  return (fake.name(), fake.address(), fake.phone_number(), fake.ascii_company_email(), fake.random_int(min=100, max=100000))

def generate_fake_data(num_records: int):
  data = []
  for i in range(num_records):
    data.append(_gen_row())

  return spark.createDataFrame(data = data, schema = schema)


df = generate_fake_data(1000)

df.coalesce(1).write.format('json').mode("overwrite").save(f"{path}/fake_data_landing")

json_path = dbutils.fs.ls(f"{path}/fake_data_landing")

for x in json_path:
  if x.path.endswith(".json"):
    dbutils.fs.cp(x.path, f"{fake_data_path}/{int(time.time())}.json")




