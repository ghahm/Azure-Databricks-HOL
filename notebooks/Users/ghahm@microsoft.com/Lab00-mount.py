# Databricks notebook source
# MAGIC %md 
# MAGIC ##### [노트북 사용] https://docs.microsoft.com/ko-kr/azure/databricks/notebooks/notebooks-use
# MAGIC ##### [mount 가이드] https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts (아래 내용이 최신 Update와 차이가 날 수 있기 때문에 최신 정보는 링크를 참조)

# COMMAND ----------

# --- mount an Azure Data Lake Storage Gen2 account to DBFS ---
#configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "<application-id>",
#           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "<scope-name>", key = "<key-name-for-service-credential>"),
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
#dbutils.fs.mount(
#  source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
#  mount_point = "/mnt/<mount-name>",
#  extra_configs = configs)

# 위의 <application-id> : 이전 단계에서 AAD(Azure Active Directoyr) 내 생성한 앱 --> 개요 --> 애플리케이션(클라이언트) ID 값임
# 위의 <scope-name> : 이전 단계에서 생성한 secret scope-name의 값 (예 : adlsgen2krc) 
# 위의 <key-name-for-service-credential> : 이전 단계에서 생성한 secret key의 값 (예 : adlsgen2krc-client-credential)
# 위의 <directory-id> : 이전 단계에서 AAD(Azure Active Directoyr) 내 생성한 앱 --> 개요 --> 디렉터리(테넌트) ID 값임
# 위의 <file-system-name> : ADLS Gen2 내의 file system (또는 container - blob에서 사용되는 용어이나 Storage Explorer 등에서는 file system과 container가 혼용되는 경우가 있음) 이름
# 위의 <storage-account-name> : ADLS Gen2 이름
# 위의 <mount-name> : 마운트 시킬 때 사용할 이름

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<application-id>",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "<scope-name>", key = "<key-name-for-service-credential>"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demodata@adlsgen2krc.dfs.core.windows.net/",
  mount_point = "/mnt/demodata",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/demodata

# COMMAND ----------

df = spark.read.text("/mnt/demodata/sparkhol/flight-data/flights.csv")
#df = spark.read.text("dbfs:/mnt/demodata/sparkhol/flight-data/flights.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.take(1)

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/demodata/sparkhol/flight-data/flights.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.take(1)

# COMMAND ----------

dbutils.fs.unmount("/mnt/demodata")
