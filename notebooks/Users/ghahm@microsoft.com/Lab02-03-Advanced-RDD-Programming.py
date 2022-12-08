# Databricks notebook source
# MAGIC %md ##### <분석 요건> 1500 마일을 초과하는 항공편의 항공기 모델명과 운항 횟수

# COMMAND ----------

# 3.1 RDD 생성하고 자료 조회하기
flightRdd = sc.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
#flightRdd.take(1)
flightRdd.count()

# COMMAND ----------

# 3.2. Distance 1,500 마일 이상 return -> (TailNum, 1) return
longDistanceFlightRDD = flightRdd.filter(lambda line: int(line[14]) > 1500). \
                                                map(lambda line:(line[7],1)) # Line[7] = TailNum, line[14] = Distance
#longDistanceRDD.take(5)
longDistanceFlightRDD.count()

# COMMAND ----------

# 3.3 TailNum를 key로 reduceByKey with + 결과 return -> sorting을 위해 key 값 변경 -> sorting -> 결과 출력을 위해 key 값 변경
longDistanceFlightRDD = longDistanceFlightRDD.reduceByKey(lambda a, b: a+b). \
                                                                map(lambda x:(x[1],x[0])). \
                                                                sortByKey(ascending=False). \
                                                                map(lambda x:(x[1],x[0]))
longDistanceFlightRDD.take(10) # TailNum, 1,500 마일 이상 운항 횟수 (내림차순)

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

# 3.5 planeDataRDD & longDistanceFlightRDD 조인
longDistanceAiplaneModelJoinRDD = planeDataRDD.join(longDistanceFlightRDD) # Join
longDistanceAiplaneModelJoinRDD.take(5)
#longDistanceAiplaneModelJoinRDD.count()

# COMMAND ----------

# 3.6 planeDataRDD & longDistanceFlightRDD 조인
longDistanceAiplaneModelJoinRDD = planeDataRDD.join(longDistanceFlightRDD) # Join
longDistanceAiplaneModelJoinRDD.take(5)
#longDistanceAiplaneModelJoinRDD.count()

# COMMAND ----------

# 3.7 조인 결과 튜플에서 모델 정보와 1,500 마일 이상 횟수 RDD return (예: (u'N14214', (u'737-824', 8)) -> (u'737-824', 8) return)
longDistanceAiplaneModelRDD = longDistanceAiplaneModelJoinRDD.map(lambda line: line[1])
longDistanceAiplaneModelRDD.take(5)

# COMMAND ----------

# 3.8 항공기 Model 명을 key로 reduceByKey with + 결과 return -> sorting을 위해 key 값 변경 -> sorting -> 결과 출력을 위해 key 값 변경
longDistanceAiplaneModelReducedRDD = longDistanceAiplaneModelRDD.reduceByKey(lambda a, b: a+b). \
                                                                                                map(lambda x: (x[1], x[0])). \
                                                                                                sortByKey(ascending=False). \
                                                                                                map(lambda x: (x[1], x[0]))
longDistanceAiplaneModelReducedRDD.take(5)
