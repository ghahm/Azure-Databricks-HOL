# Databricks notebook source
# 사전 준비 사항
# (1) Azure SQL DB 생성
# (2) Azure SQL 서버의 방화벽에서 '퍼블릭 네트워크 액세스 거부'를 아니요로 하고 테스트할 것
# (3) DB 내에 'DfTest' 테이블 생성 (쿼리는 맨 아래 참조)
from pyspark.sql import Row

# 1.1 샘플 데이터를 이용하여 테스트용 Dataframe 생성하기
# 샘플 데이터 생성 (스키마 : 'Date', 'UserID', 'DailyUserLoginCnt')
rows = [('1985-09-19', 'Song Joong-ki', 3), ('1982-02-26', 'Song Hye-kyo', 2), ('1993-06-16', 'Park Bo-gum', 3)] # 입력 데이터 
testDF = sqlContext.createDataFrame( rows, ['Date', 'UserID', 'DailyUserLoginCnt'] ) # DataFrame 명 -> RDB 테이블 명과 동일해야 함
#testDF.printSchema()
testDF.collect()

# COMMAND ----------

# 1.2 SQL 서버 접속 정보 및 save mode 설정
jdbcHostname = "<sql db host name>"
jdbcDatabase = "<db name>"
jdbcPort = 1433
jdbcUsername = "<db user>"
jdbcPassword = "<db user password>"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# save mode 참고 사항 ( http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=jdbc#pyspark.sql.DataFrameWriter.jdbc 참조 )
# (1) append : Append contents of this DataFrame to existing data (Primary key의 Dup이 없는 레코드만 정상 insert되고 나머지는 Exception Throw)
# (2) overwrite : Overwrite existing data (반드시 주의 : DB 레코드 중 DF와 Mapping 되는 레코드만 overwirte하는게 아니라 전체 테이블을 DF로 바꿔침)
# (3) ignore : Silently ignore this operation if data already exists (Primary key의 Dup이 있으면 1개도 insert되지 않고 Exception은 Throw 안함.)
# (4) error (default case): Throw an exception if data already exists (ignore의 액션과 동일하지만 ignore와는 다르게 Exception을 Throw)
save_mode = "append"
#save_mode = "overwrite"
#save_mode = "ignore"
#save_mode = "error"

# 1.3 Insert into external RDB
try :
  testDF.write.jdbc(jdbcUrl, "DfTest", mode=save_mode , properties=connectionProperties)
except Exception as e:
  print ("Write Error: ",  e)

# 1.4 저장한 RDB에 접속하여 insert 여부 확인

# COMMAND ----------

# 1.3 쿼리 테스트
pushdown_query = "(SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'DfTest') sample_alias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# 1.4 쿼리 테스트
pushdown_query = "(SELECT * FROM DfTest) sample_alias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# <참고- SQL DB에서 테스트용 테이블 생성을 위한 >
CREATE TABLE DfTest(
Date datetime NOT NULL,
UserID varchar(100) NOT NULL,
DailyUserLoginCnt int NOT NULL, 
CONSTRAINT DfTest_PK PRIMARY KEY (UserID)
)
