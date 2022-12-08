# Databricks notebook source
# 1. RDD 생성
flightRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
flightRdd.getNumPartitions()

# COMMAND ----------

# 2. RDD 파티션 개수 조회
flightsKVRdd=flightRdd.keyBy(lambda line: (line[5]))
flightsKVRdd.getNumPartitions() # flightRdd.getNumPartitions()의 파티션 결과와 동일

# COMMAND ----------

# 3. 추가 RDD 생성
carrierRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/carriers.csv"). \
                    map(lambda line: line.split(",")). \
                    map(lambda line: (line[0], line[1]))

# COMMAND ----------

# 4. 조인 수행
# 4.1 Spark Web UI 실행 하기 : Yarn -> 해당 Application의 Tracking UI 컬럼에서 ApplicationMaster 클릭
# 4.2 Join stage 보기 : Spark Web UI에서 Stage Id 0 (join) 클릭 -> DAG Visualizaion와 기타 Summary 정보 조회
# 4.3 Count stage 보기 : Spark Web UI에서 Stage Id 1 (count) 클릭 -> DAG Visualizaion와 기타 Summary 정보 조회
joinedRdd = flightsKVRdd.join(carrierRdd) 
#joinedRdd.count() 
joinedRdd.getNumPartitions() 

# COMMAND ----------

# 5. repartition 실행 후 파티션 개수 조회
flightspartKVRdd=flightsKVRdd.repartition(10)
flightspartKVRdd.getNumPartitions() 

# COMMAND ----------

# 6. 조인 수행
# 6.1 추가 실행된 3개의 Stage 정보 조회
flightspartKVRdd.join(carrierRdd).count()
