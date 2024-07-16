# Databricks notebook source
dbutils.widgets.text("name", "")

# COMMAND ----------

name = dbutils.widgets.get("name")

print(f"Hello {name}, welcome to jobs as SP")
