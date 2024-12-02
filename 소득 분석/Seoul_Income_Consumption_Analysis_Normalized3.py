# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, min, max, lit

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Seoul Market Analysis Min-Max Normalization with Rounding and Sorting") \
        .getOrCreate()

# CSV 파일 로드
file_path = "/user/maria_dev/Seoul_Market_Analysis_Income_Consumption_utf8.csv"
data = spark.read.option("header", True).option("charset", "UTF-8").csv(file_path)

# 식료품 지출 비율 계산
data_with_ratio = data.withColumn(
    "식료품_지출_비율",
        (col("식료품_지출_총금액").cast("double") / col("지출_총금액").cast("double")) * 100
        )

# 지역구별 평균 계산 및 소수점 2자리 반올림
region_summary = data_with_ratio.groupBy("행정동_코드_명").agg(
    round(avg("식료품_지출_비율"), 2).alias("평균_식료품_지출_비율")
    )

# 평균_식료품_지출_비율의 최소값과 최대값 계산
stats = region_summary.agg(
    min("평균_식료품_지출_비율").alias("최소값"),
    max("평균_식료품_지출_비율").alias("최대값") ).collect()

min_value = stats[0]["최소값"]
max_value = stats[0]["최대값"]

# Min-Max 정규화 적용
normalized_summary = region_summary.withColumn(
    "정규화된_평균_식료품_지출_비율",
        round(
                (col("평균_식료품_지출_비율") - lit(min_value)) / (lit(max_value) - lit(min_value)),
                        2
                            )
        )

# 내림차순으로 정렬
sorted_normalized_summary = normalized_summary.orderBy(
    col("정규화된_평균_식료품_지출_비율").desc()
    )

# 데이터 병합 및 저장
output_path = "/user/maria_dev/Seoul_Market_Analysis_Normalized_Sorted_Summary"
sorted_normalized_summary.coalesce(1).write.option("charset", "UTF-8").csv(output_path, header=True)

