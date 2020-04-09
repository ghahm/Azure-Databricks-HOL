# Databricks notebook source
# MAGIC %md ##### <분석 요건> 15분을 초과하는 지연 사례가 있었던 항공사

# COMMAND ----------

# 2.1 RDD 생성하고 자료 조회하기
flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
#flightRdd.take(1)
flightRdd.count()

# COMMAND ----------

# 2.2 지연 시간 15분 초과 항공편 return -> UniqueCarrier, DepDelay return
delayedFlightRDD = flightRdd.filter(lambda line: int(line[11]) > 15). \
                                        map(lambda line:(line[5],line[11])) # UniqueCarrier, DepDelay
delayedFlightRDD.take(5)
#delayedFlightRDD.count()

# COMMAND ----------

# 2.3 UniqueCarrier를 key로 reduceByKey with max() 결과 return -> sorting을 위해 key 값 변경 -> sorting -> 결과 출력을 위해 key 값 변경
longestDepartureDelayRDD = delayedFlightRDD.reduceByKey(lambda a, b: max(int(a), int(b))). \
                                                                map(lambda x:(x[1],x[0])). \
                                                                sortByKey(ascending=False). \
                                                                map(lambda x:(x[1],x[0]))
longestDepartureDelayRDD.collect()