# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Seoul Market Analysis Sorted Output") \
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

# 내림차순 정렬
sorted_summary = region_summary.orderBy(col("평균_식료품_지출_비율").desc())

# 데이터 병합 및 저장
output_path = "/user/maria_dev/Seoul_Market_Analysis_Summary_Sorted"
sorted_summary.coalesce(1).write.option("charset", "UTF-8").csv(output_path, header=True)
