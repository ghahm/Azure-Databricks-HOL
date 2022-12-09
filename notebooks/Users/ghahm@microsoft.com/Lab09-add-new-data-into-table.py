# Databricks notebook source
# 1. 1번 데이터 파일을 읽어 Dataframe 생성하고 자료 조회하기
#usersDF1 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users01.json.gz")
usersDF1 = spark.read.option("multiline", "true").json("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/json-data/users01.json.gz")
usersDF1.printSchema()
usersDF1.show()

# COMMAND ----------

# 2. Dataframe을 Permanent 테이블로 저장하기 (overwrite)
usersDF1.write.mode("overwrite").saveAsTable("users") # Managed Overwrite
#usersDF1.write.mode("overwrite").option("path","<your-s3-path>").saveAsTable("<example-table>")  # (예시) Unmanaged Overwrite

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from users order by id asc

# COMMAND ----------

# 3. 2번 데이터 파일을 읽어 Dataframe 생성 --> 기존 테이블에 추가하기 (append)
#usersDF2 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users02.json.gz")
usersDF2 = spark.read.option("multiline", "true").json("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/json-data/users02.json.gz")
usersDF2.write.mode("append").saveAsTable("users") # Managed Overwrite

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from users order by id asc

# COMMAND ----------

# 4. 3번 데이터 파일을 읽어 Dataframe 생성 --> 기존 테이블에 추가하기 (append)
#usersDF3 = spark.read.option("multiline", "true").json("/mnt/demodata/sparkhol/json-data/users03.json.gz")
usersDF3 = spark.read.option("multiline", "true").json("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/json-data/users03.json.gz")
usersDF3.write.mode("append").saveAsTable("users") # Managed Overwrite

# COMMAND ----------

%sql
select id, first_name, last_name, email, gender, ip_address from users order by id asc

# 추가 테스트
# 4.을 overwrite로 변경하여 다시 실행해 보고 select를 실행한 데이터 결과 비교 --> overwrite의 결과 (DF을 기준으로 action이 실행됨을 기억할 것)
# 4.을 overwrite로 변경하여 다시 실행해 보고 select를 실행한 데이터 결과 비교 --> 동일한 데이터를 append 했을 때의 결과
# 4.을 error로 변경하여 다시 실행해 보고 select를 실행한 데이터 결과 비교
# 4.을 igngore로 변경하여 다시 실행해 보고 select를 실행한 데이터 결과 비교 --> error와의 차이 (exception 처리)

# 참고 : http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=jdbc#pyspark.sql.DataFrameWriter.mode
# Lab07의 jdbc save 모드와는 약간 차이가 있음 (PK의 존재 여부에 따른 것으로 보임)
# append: Append contents of this DataFrame to existing data.
# overwrite: Overwrite existing data.
# error: Throw an exception if data already exists.
# ignore: Silently ignore this operation if data already exists.
