# Databricks notebook source
# MAGIC %md ##### Create a delta table - Python

# COMMAND ----------

# 1. 소스 데이터 파일을 읽어 Dataframe 생성하고 자료 조회하기
#usersDF = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/all-users.json.gz")
usersDF = spark.read.option("multiline", "true").json("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/json-data/all-users.json.gz")
#usersDF.printSchema()
usersDF.show()

# COMMAND ----------

# 2. Delta 테이블 생성하기 --> overwrite / append
usersDF.write.format("delta").mode("overwrite").save("abfss://demodata@ghadlskrc.dfs.core.windows.net/delta/users") # Managed Overwrite
#usersDF.write.format("delta").mode("append").save("abfss://demodata@ghadlskrc.dfs.core.windows.net/delta/users") # Managed Overwrite
#usersDF.write.format("delta").mode("overwrite").save("abfss://demodata@ghadlskrc.dfs.core.windows.net/delta/users") # Managed Overwrite
#usersDF.write.format("delta").mode("append").save("abfss://demodata@ghadlskrc.dfs.core.windows.net/delta/users") # Managed Overwrite
spark.sql("CREATE TABLE delta_df_users USING DELTA LOCATION 'abfss://demodata@ghadlskrc.dfs.core.windows.net/delta/users'")

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from delta_df_users order by id asc

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from delta.`/mnt/demodata/delta/users` order by id asc

# COMMAND ----------

#%sql
#drop table if exists delta_df_users
