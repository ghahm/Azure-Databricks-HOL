# Databricks notebook source
# MAGIC %md ##### <분석 요건> 15분을 초과하는 지연 사례가 있었던 항공사

# COMMAND ----------

from pyspark.sql.types import *

# 2.1 RDD 생성하고 자료 조회하기
#flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
flightRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
#flightRdd.take(1)
flightRdd.count()

# COMMAND ----------

# 2.2 스키마 및 파싱 정보 정의 -> DataFrame 생성 -> TempTable 생성 
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

# COMMAND ----------

# 쿼리 수행을 위하여 DataFrame을 임시 테이블로 등록
flightDF.createOrReplaceTempView("flightTable")

#flightDF.show()
flightDF.take(5)

# COMMAND ----------

# 2.3 sqlContext를 이용하여 쿼리 수행 
resultDF = sqlContext.sql(" SELECT UniqueCarrier, max(DepDelay) as maxDelayedTime \
                                    FROM ( \
                                           SELECT UniqueCarrier, DepDelay \
                                          FROM flightTable \
                                          WHERE DepDelay > 15 \
                                    ) d \
                                    GROUP BY UniqueCarrier \
                                    ORDER BY maxDelayedTime DESC ")
resultDF.collect()

# COMMAND ----------

# 2.4 %%sql magic 정의 -> 쿼리 수행 (주의 : 아래 sql magic이 포함된 셀에는 # comment를 추가하면 에러 발생하기 때문에 별도의 셀로 작성)

# COMMAND ----------

%sql
SELECT UniqueCarrier, max(DepDelay) as maxDelayedTime
FROM (
      SELECT UniqueCarrier, DepDelay
      FROM flightTable
      WHERE DepDelay > 15
) d
GROUP BY UniqueCarrier
ORDER BY maxDelayedTime DESC
