# Databricks notebook source
# MAGIC %md ##### <분석 요건> 운항 편수 Top 10 항공사 조회

# COMMAND ----------

from pyspark.sql.types import *

# 1. RDD 생성하고 자료 조회하기
#flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
flightRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
flightRdd.take(1)

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


# 매핑
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
flightDF.registerTempTable("flightTable")

#flightDF.show()
flightDF.take(5)

# COMMAND ----------

# 1.3 sqlContext를 이용하여 쿼리 수행 (주의 : 코드를 Copy하면 쿼리 사이에 빈 라인이 생겨 에러 발생. 해당 빈 라인을 삭제할 )
resultDF = sqlContext.sql("SELECT UniqueCarrier, count(UniqueCarrier) as cnt \
                        FROM flightTable \
                        GROUP BY UniqueCarrier \
                        ORDER BY cnt DESC")
resultDF.collect()

# COMMAND ----------

# 1.4 %sql magic 정의 -> 쿼리 수행 
# (주의 : 아래 sql magic이 포함된 셀에는 # comment를 추가하면 에러 발생하기 때문에 별도의 셀로 작성)
# (주의 : 아래 sql 앞의 %는 HDI Spark에서는 %%로 Databricks에서는 %로 할 )

# COMMAND ----------

%sql
SELECT UniqueCarrier, count(UniqueCarrier) as cnt 
FROM flightTable
GROUP BY UniqueCarrier
ORDER BY cnt DESC
