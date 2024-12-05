from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# SparkSession 생성
spark = SparkSession.builder.appName("MarketDensity_Calculation").getOrCreate()

# 데이터 경로 설정
market_sum_path = "/user/maria_dev/market_density/Market_Sum.csv"
population_density_sum_path = "/user/maria_dev/market_density/PopulationDensity_Sum.csv"

# 데이터 읽기
market_sum = spark.read.csv(market_sum_path, header=True, inferSchema=True, encoding="utf-8")
population_density_sum = spark.read.csv(population_density_sum_path, header=True, inferSchema=True, encoding="utf-8")

# 두 데이터프레임을 자치구 기준으로 Inner Join
merged_data = market_sum.join(
    population_density_sum,
    market_sum["자치구"] == population_density_sum["자치구"],
    "inner"
).select(
    market_sum["자치구"],
    col("시장 수"),
    col("인구밀도")
)

# 시장밀도 계산: 시장 수 / 인구밀도
market_density = merged_data.withColumn(
    "시장밀도",
    col("시장 수") / col("인구밀도")
).select(
    col("자치구"),
    col("시장밀도")
)

# 결과를 단일 파일로 저장
output_path = "/user/maria_dev/market_density/MarketDensity.csv"
market_density.coalesce(1).write.csv(output_path, header=True)

print("MarketDensity 저장이 완료되었습니다.")