# Databricks notebook source
  # Spark Web UI의 Stage 메뉴에서 task 개수를 참고하여 파티션 수 확인 -> 총 파일 수 : 1.6 MB 파일 1개  --> RDD 파티션 2개
ordersRdd1=spark.sparkContext.textFile("/mnt/demodata/sparkhol/rdd-partition-data/ordersTbls/small-1/orders.tbl").map(lambda line: line.split("|")) 
ordersRdd1.getNumPartitions()

# COMMAND ----------

ordersRdd1.count()

# COMMAND ----------

# Spark Web UI의 Stage 메뉴에서 task 개수를 참고하여 파티션 수 확인 -> 총 파일 수 : 1.6 MB 파일 2개  --> RDD 파티션 2개
ordersRdd2=spark.sparkContext.textFile("/mnt/demodata/sparkhol/rdd-partition-data/ordersTbls/small-[1-2]/orders.tbl").map(lambda line: line.split("|")) 
ordersRdd2.getNumPartitions()

# COMMAND ----------

ordersRdd2.count()

# COMMAND ----------

# Spark Web UI의 Stage 메뉴에서 task 개수를 참고하여 파티션 수 확인 -> 총 파일 수 : 1.6 MB 파일 3개  --> RDD 파티션 3개
ordersRdd3=spark.sparkContext.textFile("/mnt/demodata/sparkhol/rdd-partition-data/ordersTbls/small-[1-3]/orders.tbl").map(lambda line: line.split("|")) 
ordersRdd3.getNumPartitions()

# COMMAND ----------

ordersRdd3.count()

# COMMAND ----------

# Spark Web UI의 Stage 메뉴에서 task 개수를 참고하여 파티션 수 확인 -> 총 파일 수 : 1.6 MB 파일 7개 & 164 MB 1개 --> RDD 파티션 9개 (1.6 MB 7개와 128 MB + 32MB 2개로 나뉘어짐어짐) 
ordersRdd4=spark.sparkContext.textFile("/mnt/demodata/sparkhol/rdd-partition-data/ordersTbls/*/orders.tbl").map(lambda line: line.split("|")) 
ordersRdd4.getNumPartitions()

# COMMAND ----------

ordersRdd4.count()

# COMMAND ----------

# Jupyter의 Spark UI의 Storage 메뉴에서 파티션 수 확인 -> 총 파일 수 : 1.6 MB 파일 7개 & 164 MB 1개 --> RDD 파티션 9개 (업데이트 되는데 시간이 다소 소요됨)
ordersRdd4.cache()
ordersRdd4.count()

# COMMAND ----------

ordersRdd4.count()
