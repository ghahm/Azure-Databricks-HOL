# Databricks notebook source
# MAGIC %md ##### <분석 요건> 운항 편수 Top 10 항공사 조회

# COMMAND ----------

# 1.1 RDD 생성하고 자료 조회하기
flightRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/flight-data/flights.csv").map(lambda line: line.split(",")) 
flightRdd.take(1)

# COMMAND ----------

# 1.2. flightRdd의 6번째 열에 Integer 값을 assign (운항 편수 Top 10 항공사 조회를 위한 전처리)
carrierRdd = flightRdd.map(lambda line: (line[5],1)) # UniqueCarrier
carrierRdd.take(10)

# COMMAND ----------

# 1.3. carrierRdd의 각 Key 별로 aggregation을 수행하여 각 단어의 개수 계산하기
carrierRdd = carrierRdd.reduceByKey(lambda a, b: a+b)o
carrierRdd.take(10)

# COMMAND ----------

# 1.4. carrierRdd의 Key를 기준으로 내림차순 정렬 수행 (항공사 명 기준으로 정렬)
carrierRdd = carrierRdd.sortByKey(ascending=False)
carrierRdd.take(10)

# COMMAND ----------

# 1.5. 운항 편수 Top 10 항공사 조회 (운항 편수 기준으로 내림차순 정렬하기 위해 carrierRdd의 key, value의 순서를 바꾸고 내림차순 정렬 재실행)
carrierRdd = carrierRdd.map(lambda x:(x[1],x[0])). \
                                sortByKey(ascending=False)
carrierRdd.take(10) 

# COMMAND ----------

# 1.6. (통합 코드) 운항 편수 Top 10 항공사 조회 -> 1.2 ~ 1.5를 통합 코드로
carrierRdd = flightRdd.map(lambda line: (line[5],1)). \
                                reduceByKey(lambda a, b: a+b). \
                                map(lambda x:(x[1],x[0])). \
                                sortByKey(ascending=False). \
                                map(lambda x:(x[1],x[0]))
carrierRdd.take(10)