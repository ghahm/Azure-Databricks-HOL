# Databricks notebook source
from pyspark import StorageLevel

# 1. RDD 생성하고 join하기
flightRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/flights.csv"). \
                    map(lambda line: line.split(",")). \
                    keyBy(lambda line: (line[5]))
carrierRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/flight-data/carriers.csv"). \
                    map(lambda line: line.split(",")). \
                    map(lambda line: (line[0], line[1]))
joinedRdd = flightRdd.join(carrierRdd)
joinedRdd.count()

# COMMAND ----------

# 2. caching 수행
joinedRdd.cache()
joinedRdd.count()

# COMMAND ----------

# 3. persisting (or caching) 수행 이전과 이후 수행 결과를 Spark UI의 Stages에서 수행 시간 차이를 확인
joinedRdd.count()

# COMMAND ----------

# 4. unpersist() 수행
joinedRdd.unpersist()

# COMMAND ----------

joinedRdd.count()

# COMMAND ----------

# 5. persisting 수행
joinedRdd.persist(StorageLevel.DISK_ONLY)
joinedRdd.count()

# COMMAND ----------

# 6. persisting (or caching) 수행 이전과 이후 수행 결과를 Spark UI의 Stages에서 수행 시간 차이를 확인
joinedRdd.count()
