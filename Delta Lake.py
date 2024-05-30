# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].replace(".", "")
print(user_name)

dbutils.widgets.text("catalogformatted", user_name)
dbutils.widgets.text("schemaformatted", user_name)
dbutils.widgets.combobox("create_catalog", "false", ["true", "false"], "Create Catalog")

# COMMAND ----------

catalog = dbutils.widgets.get("catalogformatted")
schema = dbutils.widgets.get("schemaformatted")

# COMMAND ----------

if dbutils.widgets.get("create_catalog") == "true":
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalogformatted};
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ${schemaformatted};
# MAGIC USE SCHEMA ${schemaformatted};
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

print(f"Current Catalog: {_sqldf.collect()[0][0]}")
print(f"Current Schema: {_sqldf.collect()[0][1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Let's Generate some Fake Data for use in this lab

# COMMAND ----------

# MAGIC %run ./include/generate_data 

# COMMAND ----------

# MAGIC %md
# MAGIC # Let's take a look at the file that was generated

# COMMAND ----------

display(dbutils.fs.ls(f"{path}/fake_data/"))

# COMMAND ----------

dbutils.fs.head(dbutils.fs.ls(f"{path}/fake_data/")[0].path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OK,  but that is hard to read can we read that in as a table...?

# COMMAND ----------

df = spark.read.format("json").load(f"{path}/fake_data")
display(df)

# COMMAND ----------

print(fake_data_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Update Path from above cell
# MAGIC SELECT * FROM json.`dbfs:/tmp/craig.kovar@databricks.com/fake_data`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's save it as a Delta table.

# COMMAND ----------

spark.read.format("json").load(f"{path}/fake_data").write.mode("overwrite").saveAsTable("fake_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fake_data
# MAGIC AS SELECT * from json.`dbfs:/tmp/craig.kovar@databricks.com/fake_data`

# COMMAND ----------

spark.table("fake_data").limit(10).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED fake_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fake_data

# COMMAND ----------

# DBTITLE 1,Let's delete from table where spend is < 2000 .... Oops
# MAGIC %sql
# MAGIC DELETE FROM fake_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Time travel to the rescue

# COMMAND ----------

df2 = spark.read.table("fake_data").display()

# COMMAND ----------

#df1 = spark.read.option("timestampAsOf", "2019-01-01").table("people10m")
df2 = spark.read.option("versionAsOf", 0).table("fake_data").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM people10m TIMESTAMP AS OF '2018-10-18T22:15:12.013Z'
# MAGIC SELECT * FROM fake_data VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,And now lets fix our mistakes
# MAGIC %sql
# MAGIC RESTORE TABLE fake_data TO VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,Exercise 1
# MAGIC %sql
# MAGIC -- Delete all the rows where spend is < 5000 and then restore them back
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Ok now it is time to define our silver tables

# COMMAND ----------

# DBTITLE 1,Define Pandas UDF to extract state from Address
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType

@pandas_udf("string")
def extract_state(address: pd.Series) -> pd.Series:
    # Use the series.str.extract() method to extract the state from the address string
    # The regular expression '(?P<state>[A-Z]{2})' matches two uppercase letters (i.e., the state abbreviation)
    return address.str.extract('(?P<state>[A-Z]{2})', expand=False)

# COMMAND ----------

# DBTITLE 1,Let's add state column
df = spark.read.table("fake_data").withColumn("state", extract_state(col("address")))

display(df)

# COMMAND ----------

# We can also use built in functions
df2 = spark.read.table("fake_data").withColumn("state", col("address").substr(-8,8).substr(0,2))
display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fake_data WHERE email is NULL

# COMMAND ----------

# DBTITLE 1,Exercise 2 - Let's also filter out the rows where email is NULL
#To Do - Filter out the rows where the email is null
#. ** Filter is a hint**

silver_df = df.<FILL_IN>

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # We are now ready to make some gold tables to serve to the business.  We will make the following tables
# MAGIC
# MAGIC * Summary of spend per state
# MAGIC * Masked table where email is masked using sha2 function
# MAGIC
# MAGIC *Please note this is a simple example.  For true Row and Column level filtering and masking we would use the UC Row and Column Level Security documented at*
# MAGIC
# MAGIC [Row and Column Filters](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/row-and-column-filters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additionally to remove column we can use .drop() method

# COMMAND ----------

from pyspark.sql.functions import sha2

gold_df = df.withColumn("masked_email",sha2("email", 256)).drop("email")
gold_df.write.mode("overwrite").saveAsTable("gold_masked_table")
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Exercise 3 - Create State summarization table
# To Do - Create Gold Table with the total spend per State

# COMMAND ----------

# MAGIC %md
# MAGIC # Ok,  great we read a directory and created some tables but can we improve efficience and only process new data?
# MAGIC
# MAGIC Yes,  let's talk about Autoloader and Change Data Feed
# MAGIC
# MAGIC [Autoloader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", f"{path}/schema")
  .load(f"{path}/fake_data")
  .writeStream
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", f"{path}/checkpoint")
  .trigger(availableNow=True)
  .table("al_bronze_table")
)

# COMMAND ----------

from pyspark.sql.functions import col

slv_al_df = (spark
 .readStream
 .table("al_bronze_table")
 .withColumn("state", col("address").substr(-8,8).substr(0,2))
 .filter("email is NOT NULL")
 .writeStream
 .outputMode("append")
 .option("mergeSchema", "true")
 .option("checkpointLocation", f"{path}/al_checkpoint")
 .trigger(availableNow=True)
 .table("al_silver_table")
 )

# COMMAND ----------

# MAGIC %md
# MAGIC # Ok,  can we make this more declarative?  .... The answer is yes using Delta Live Tables
