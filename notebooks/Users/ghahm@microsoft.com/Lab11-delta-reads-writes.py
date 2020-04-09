# Databricks notebook source
# MAGIC %md ##### Query an older snapshot of a table (time travel) - https://docs.databricks.com/delta/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel

# COMMAND ----------

# Data --> Table --> History에서 버전 확인 가능
# 신규 write operation 발생 시, versioning 발생
# Data retention은 기본값 30일. table properties에서 변경 가능

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from delta_df_users version as of 0

# COMMAND ----------

# MAGIC %md ##### Table properties - https://docs.databricks.com/delta/delta-batch.html#table-properties-1

# COMMAND ----------

# You can store your own metadata as a table property using TBLPROPERTIES in CREATE and ALTER.
# TBLPROPERTIES are stored as part of Delta table metadata. You cannot define new TBLPROPERTIES in a CREATE statement if a Delta table already exists in a given location. See table creation for more details.
# Block deletes and modifications of a table: delta.appendOnly=true.
# Configure the number of columns for which statistics are collected: delta.dataSkippingNumIndexedCols=<number-of-columns>. This property takes affect only for new data that is written out.
# Configure the time travel retention properties: delta.logRetentionDuration=<interval-string> and delta.deletedFileRetentionDuration=<interval-string>

# COMMAND ----------

# MAGIC %md ##### Table metadata - https://docs.databricks.com/delta/delta-batch.html#table-metadata-1

# COMMAND ----------

# Delta Lake has rich features for exploring table metadata. It supports Show Partitions, Show Columns, Describe Table, and so on. It also provides the following unique commands:
