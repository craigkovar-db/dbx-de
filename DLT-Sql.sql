-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE dlt_sql_bronze ()
COMMENT "DLT SQL Autoloader Table"
AS SELECT * FROM cloud_files("dbfs:/tmp/craig.kovar@databricks.com/fake_data", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE dlt_sql_silver (
  CONSTRAINT email_not_null EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT *, extract_state(address) as `state` FROM STREAM(live.dlt_sql_bronze)

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE dlt_sql_gold_mask()
AS SELECT address, name, phone, total_spend, sha2(email,256) as masked_email FROM STREAM(live.dlt_sql_silver)

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE dlt_sql_gold_agg()
AS SELECT state, sum(total_spend) as state_spend FROM live.dlt_sql_silver GROUP BY state
