# Databricks notebook source
dbutils.fs.ls("abfss://demodata@adlsgen2krc.dfs.core.windows.net/")

# COMMAND ----------

df = spark.read.text("abfss://demodata@adlsgen2krc.dfs.core.windows.net/sparkhol/flight-data/flights.csv")

# COMMAND ----------

df.take(1)