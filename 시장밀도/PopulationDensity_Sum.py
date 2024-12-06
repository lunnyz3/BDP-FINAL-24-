from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# SparkSession 생성
spark = SparkSession.builder.appName("PopulationDensity_Sum").getOrCreate()

# 데이터 경로 설정
market_status_path = "/user/maria_dev/market_density/population_density.csv"

# 데이터 읽기
market_status = spark.read.csv(market_status_path, header=True, inferSchema=True, encoding="utf-8")

# 자치구별 점포수(개소) 열 추출 및 소계 필터링
filtered_data = market_status.filter(
    (col("동별(3)") == "소계") & (col("동별(2)") != "소계")
).select(
    col("동별(2)").alias("자치구"),
    col("20235").alias("인구밀도")
)

# 결과를 단일 파일로 저장
output_path = "/user/maria_dev/market_density/PopulationDensity_Sum.csv"
filtered_data.coalesce(1).write.csv(output_path, header=True)

print("PopulationDensity_Sum 저장이 완료되었습니다.")