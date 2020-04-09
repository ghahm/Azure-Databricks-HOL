# Databricks notebook source
# 1. 1번 데이터 파일을 읽어 Dataframe 생성하고 자료 조회하기
usersDF1 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users01.json.gz")
usersDF1.printSchema()
usersDF1.show()

# COMMAND ----------

# 2. Dataframe을 Permanent 테이블로 저장하기 (overwrite)
usersDF1.write.mode("overwrite").saveAsTable("users") # Managed Overwrite
#usersDF1.write.mode("overwrite").option("path","<your-s3-path>").saveAsTable("<example-table>")  # (예시) Unmanaged Overwrite

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, first_name, last_name, email, gender, ip_address from users order by id asc

# COMMAND ----------

# 3. 2번 데이터 파일을 읽어 Dataframe 생성 --> 기존 테이블에 추가하기 (append)
usersDF2 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users02.json.gz")
usersDF2.write.mode("append").saveAsTable("users") # Managed Overwrite

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, first_name, last_name, email, gender, ip_address from users order by id asc

# COMMAND ----------

# 4. 3번 데이터 파일을 읽어 Dataframe 생성 --> 기존 테이블에 추가하기 (append)
usersDF3 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users03.json.gz")
usersDF3.write.mode("append").saveAsTable("users") # Managed Overwrite

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, first_name, last_name, email, gender, ip_address from users order by id asc