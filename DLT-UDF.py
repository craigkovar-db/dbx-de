# Databricks notebook source
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType

@pandas_udf("string")
def extract_state_udf(address: pd.Series) -> pd.Series:
    # Use the series.str.extract() method to extract the state from the address string
    # The regular expression '(?P<state>[A-Z]{2})' matches two uppercase letters (i.e., the state abbreviation)
    return address.str.extract('(?P<state>[A-Z]{2})', expand=False)

# COMMAND ----------

spark.udf.register("extract_state", extract_state_udf)
