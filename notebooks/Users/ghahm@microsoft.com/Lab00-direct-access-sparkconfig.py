storage_account_name = "STORAGE_ACCOUNT_NAME"
storage_account_access_key = "YOUR_ACCESS_KEY"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

file_location = "wasbs://demodata@ghadlskrc.blob.core.windows.net/sparkhol/flight-data/carriers.csv"
file_type = "csv"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
display(df)
