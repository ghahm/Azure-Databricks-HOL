# Databricks notebook source
# MAGIC %md ##### <분석 요건> 1500 마일을 초과하는 항공편의 항공기 모델명과 운항 횟수

# COMMAND ----------

from pyspark.sql.types import *

# 3.1 RDD 생성하고 자료 조회하기
flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
#flightRdd.take(1)
flightRdd.count()

# COMMAND ----------

# 3.2 스키마 및 파싱 정보 정의 -> DataFrame 생성 -> TempTable 생성 
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
flightDF.registerTempTable("flightTable")

#flightDF.collect()
flightDF.take(5)

# COMMAND ----------

# 3.3 flightTable : %%sql magic 정의 -> 쿼리 수행 (주의 : 아래 sql magic이 포함된 셀에는 # comment를 추가하면 에러 발생하기 때문에 별도의 셀로 작성)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TailNum, count(TailNum) as cnt 
# MAGIC FROM ( 
# MAGIC     SELECT TailNum 
# MAGIC     FROM flightTable 
# MAGIC     WHERE Distance > 1500 
# MAGIC ) longDist 
# MAGIC GROUP BY TailNum
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# 3.4 PlaneDataRDD 생성 -> 헤더 행 필터링 & split -> 각 plane 행의 정보가 부족할 경우 필터링 -> TailNum & Model return
planeDataRDD=spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/plane-data.csv")
header = planeDataRDD.first() #extract header
planeDataRDD = planeDataRDD.filter(lambda x:x !=header). \
                                            map(lambda line: line.split(",")) # filter header & split
planeDataRDD = planeDataRDD.filter(lambda x:len(x) == 9). \
                                            map(lambda line: (line[0], line[4])) # 완전한 plane 정보를 보유한 행만 filter & Tailnum , Model  만 retrun
planeDataRDD.count()

# COMMAND ----------

# 3.5 plane Table: 스키마 및 파싱 정보 정의 -> DataFrame 생성 -> TempTable 생성
# 스키마 정의
planeDataSchema = StructType([ StructField("Tailnum", StringType(), True), \
                                            StructField("Model", StringType(), True)])

# 파싱 정보 정의
planeDataParsing = planeDataRDD.map(lambda s:(str(s[0]), \
                                                                 str(s[1])))

# DataFrame 생성하기
planeDataDF = sqlContext.createDataFrame(planeDataParsing, planeDataSchema)

# 쿼리 수행을 위하여 DataFrame을 임시 테이블로 등록
planeDataDF.registerTempTable("planeDataTable")

#planeDataDF.collect()
planeDataDF.take(5)

# COMMAND ----------

# 3.6 최종 쿼리 수행

# COMMAND ----------

%sql
SELECT planeDataTable.Model, sum(cnt) as sumofcnt
FROM ( 
     SELECT longDist.TailNum, count(longDist.TailNum) as cnt
     FROM (
         SELECT flightTable.TailNum
         FROM flightTable
         WHERE Distance > 1500
     ) longDist
     GROUP BY longDist.TailNum
     ORDER BY cnt DESC
 ) longDistanceFlight
 INNER JOIN planeDataTable
 ON longDistanceFlight.TailNum=planeDataTable.Tailnum
 GROUP BY planeDataTable.Model
 ORDER BY sumofcnt DESC
