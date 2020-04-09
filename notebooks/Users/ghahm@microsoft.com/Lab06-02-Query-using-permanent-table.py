# Databricks notebook source
# MAGIC %md ##### 새로운 노트북을 열어서 실행해 보기

# COMMAND ----------

# 1.1 Table 현황 조회 (새로운 노트북이기 때문에 이전에 생성한 Temporary 테이블은 조회되지 않음)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# 1.2 운항 편수 Top 10 항공사 조회 쿼리 from Permanent 테이블 (flightdstable)
# flightdstable는 이미 Permanent 테이블로 저장되어 있어 직접 쿼리 수행 가능

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, count(UniqueCarrier) as cnt 
# MAGIC FROM flightdstable
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# 1.3 운항 편수 Top 10 항공사 조회 쿼리 from Temporary 테이블 (flightTable)
# flighttable는 이전 Lab에서 Temporary 테이블로 저장했기 때문에 새로 생성한 노트북에서는 테이블을 인식하지 못하기 때문에 에러가 발생

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, count(UniqueCarrier) as cnt 
# MAGIC FROM flightTable
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY cnt DESC