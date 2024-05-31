# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = f"dbfs:/tmp/{user_name}"
fake_data_path = f"{path}/fake_data"

dbutils.widgets.text("catalogformatted", user_name.split('@')[0].replace(".", ""))
dbutils.widgets.text("schemaformatted", user_name.split('@')[0].replace(".", ""))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalogformatted};
# MAGIC
# MAGIC USE SCHEMA ${schemaformatted};

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
from faker import Faker
import time
import random

fake = Faker()
settings = {
  "perc_insert" : 0.7,
  "perc_update" : 0.2,
  "perc_delete" : 0.1,
  "starting_id" : 0
}


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True),  
  StructField("email", StringType(), True),
  StructField("address", StringType(), True),  
  StructField("phone", StringType(), True),  
  StructField("op_type", StringType(), True),  
  StructField("txn_time", LongType(), True)
])

# COMMAND ----------

def get_op_type(type_perc:float, settings:dict) -> str:
  if type_perc < settings["perc_delete"]:
    return "D"
  elif type_perc < settings["perc_delete"] + settings["perc_update"]:
    return "U"
  
  return "I"

def gen_cdc_data(num_rec:int, settings:dict) -> list:
  data_list = []
  for i in range(num_rec):
    op_type = get_op_type(round(random.random(),2), settings)
    txn_time = round(time.time() * 1000)
    name = fake.name()
    email = fake.ascii_email()
    address = fake.address()
    phone = fake.phone_number()
    id = 0

    if op_type == "I":
      id = settings["starting_id"] + 1
      settings["starting_id"] += 1
    else:
      id = random.randint(0, settings["starting_id"])

    row = (id, name, email, address, phone, op_type, txn_time)

    data_list.append(row)

  return data_list
  

# COMMAND ----------

# DBTITLE 1,Generate New Batch of Data
df = spark.createDataFrame(schema = schema,  data = gen_cdc_data(1000, settings))

df.write.mode("append").saveAsTable("cdc_raw")

# COMMAND ----------


