# Databricks notebook source
# MAGIC %md ##### Word count 예제

# COMMAND ----------

# 1. RDD 생성하고 자료 조회하기
#baseRdd = spark.sparkContext.textFile("/mnt/demodata/sparkhol/wordcount-data/Harry-Potter-and-the-Sorcerer.txt.gz")
baseRdd = sc.textFile("abfss://demodata@ghadlskrc.dfs.core.windows.net/sparkhol/wordcount-data/Harry-Potter-and-the-Sorcerer.txt.gz")
baseRdd.take(3) # 첫번째 행 return
#baseRdd.collect() # 전체 데이터 return

# COMMAND ----------

# 2. Transformations 함수 예: map() vs. flatMap  아래 2개의 take() 결과 비교
# map() : Return a new RDD by applying a function to each element of this RDD
splitRdd01 = baseRdd.map(lambda line: line.split(" "))
splitRdd01.take(5)

# COMMAND ----------

# flatMap() : Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results
splitRdd02 = baseRdd.flatMap(lambda line: line.split(" "))
splitRdd02.take(5)

# COMMAND ----------

# 3. splitRdd02의 각 Key에 Integer 값을 assign (Word count를 위한 사전 작업)
mappedRdd = splitRdd02.map(lambda line: (line,1))
mappedRdd.take(5)

# COMMAND ----------

# 4. mappedRdd의 각 Key 별로 aggregation을 수행하여 각 단어의 개수 계산하기
# reduceByKey() : Merge the values for each key using an associative reduce function
reducedRdd = mappedRdd.reduceByKey(lambda a,b: a+b) # 각 Key의 Integer 값을 합산
reducedRdd.take(20)
#reducedRdd.collect()

# COMMAND ----------

# 5. reducedRdd를 2번째 컬럼을 기준으로 내림차순 정렬하여 Top 10 Word Count를 조회
# sortBy(keyfunc, ascending=True, numPartitions=None) : Sorts this RDD by the given keyfunc
reducedRdd.sortBy(lambda x: x[1], False).take(10)
