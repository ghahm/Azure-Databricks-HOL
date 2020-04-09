# Databricks notebook source
# MAGIC %md ##### SparkQL - #1 - registerTempTable vs saveAsTable

# COMMAND ----------

from pyspark.sql.types import *

# 1.1 RDD 생성하고 자료 조회하기
flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
#flightRdd.take(1)
flightRdd.count()

# COMMAND ----------

# 1.2 스키마 및 파싱 정보 정의 -> DataFrame 생성 -> TempTable 생성 
# 스키마 정의
flightSchema = StructType([ StructField("Month", StringType(), False), \
                                        StructField("DayofMonth", StringType(), False), \
                                        StructField("DayOfWeek", StringType(), False), \
                                        StructField("DepTime", IntegerType(), False), \
                                        StructField("ArrTime", IntegerType(), False), \
                                        StructField("UniqueCarrier", StringType(), False), \
                                        StructField("FlightNum", StringType(), False), \
                                        StructField("TailNum", StringType(), True), \
                                        StructField("ElapsedTime", IntegerType(), False), \
                                        StructField("AirTime", IntegerType(), False), \
                                        StructField("ArrDelay", IntegerType(), False), \
                                        StructField("DepDelay", IntegerType(), False), \
                                        StructField("Origin", StringType(), False), \
                                        StructField("Dest", StringType(), False), \
                                        StructField("Distance", IntegerType(), False), \
                                        StructField("TaxiIn", IntegerType(), False), \
                                        StructField("TaxiOut", IntegerType(), False), \
                                        StructField("Cancelled", IntegerType(), False), \
                                        StructField("CancellationCode", StringType(), False), \
                                        StructField("Diverted", IntegerType(), False)])


# 파싱 정보 정의
flightParsing = flightRdd.map(lambda s:(str(s[0]), \
                                                     str(s[1]), \
                                                     str(s[2]), \
                                                     int(s[3]), \
                                                    int(s[4]), \
                                                    str(s[5]), \
                                                    str(s[6]), \
                                                    str(s[7]), \
                                                    int(s[8]), \
                                                    int(s[9]), \
                                                    int(s[10]), \
                                                    int(s[11]), \
                                                    str(s[12]), \
                                                    str(s[13]), \
                                                    int(s[14]), \
                                                    int(s[15]), \
                                                    int(s[16]), \
                                                    int(s[17]), \
                                                    str(s[18]), \
                                                    int(s[19])))

# DataFrame 생성하기
flightDF = sqlContext.createDataFrame(flightParsing, flightSchema)

# 쿼리 수행을 위하여 DataFrame을 임시 테이블로 등록
flightDF.registerTempTable("flightTable")# 1.3 Table 현황 조회 (Temporary 테이블과 Permanent 테이블)

# COMMAND ----------

# 1.3 Table 현황 조회 (Temporary 테이블과 Permanent 테이블)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# 1.4 운항 편수 Top 10 항공사 조회 쿼리 from Temporary 테이블 (flightTable)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, count(UniqueCarrier) as cnt 
# MAGIC FROM flightTable
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# 1.5 쿼리 수행을 위하여 DataFrame을 영구 테이블로 저장 (Saves the content of the DataFrame as the specified table)
flightDF.write.saveAsTable("flightDSTable")

# COMMAND ----------

# 1.6 Table 현황 조회 (Temporary 테이블과 Permanent 테이블)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# 1.7 운항 편수 Top 10 항공사 조회 쿼리 from Permanent 테이블 (flightdstable)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, count(UniqueCarrier) as cnt 
# MAGIC FROM flightdstable
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# 1.8 Spark 클러스터의 Default Blob Container 내 hive/warehouse 경로 아래 flightdstable 관련 파일 생성 확인 (Databricks는 확인 불가)