#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Spark 세션 생성
spark = SparkSession.builder     .appName("Gu Distribution Analysis")     .getOrCreate()

# 데이터 디렉토리 설정
data_dir = "/home/maria_dev/spark/data_categorized/utf8/data2"

# 카테고리별 파일 목록
category_files = [
    "data_agriculture.csv",
    "data_livestock.csv",
    "data_seafood.csv",
    "data_sauce.csv",
    "data_instant.csv",
    "data_beverage.csv",
    "data_dairy.csv"
]

# 데이터 읽기
dataframes = {}
for file_name in category_files:
    file_path = "{}/{}".format(data_dir, file_name)  # f-string 대신 .format() 사용
    try:
        df = spark.read.option("header", "true")             .option("encoding", "UTF-8")             .csv(file_path)
        dataframes[file_name.replace(".csv", "")] = df
    except Exception as e:
        print("Error reading {}: {}".format(file_name, e))  # f-string 대신 .format() 사용
        dataframes[file_name.replace(".csv", "")] = spark.createDataFrame([], schema=[])

# 모든 데이터 합치기
all_data = None
for df in dataframes.values():
    all_data = df if all_data is None else all_data.union(df)

# '가격(원)'을 숫자형으로 변환
all_data = all_data.withColumn("가격(원)", col("가격(원)").cast("double"))

# 전체 자치구별 평균, 변동계수 계산 함수
def calculate_overall_gu_statistics(data):
    # 자치구별 평균 및 변동계수
    stats = data.groupBy("자치구 이름")         .agg(
            F.mean("가격(원)").alias("전체 평균 가격"),
            F.stddev("가격(원)").alias("표준편차"),
            F.count("*").alias("데이터 개수")
        )
    stats = stats.withColumn(
        "전체 변동계수(cv)",
        F.when(col("전체 평균 가격") != 0, col("표준편차") / col("전체 평균 가격")).otherwise(0)
    )
    return stats

# 년도/월별 통계 계산 함수
def calculate_gu_distribution_and_volatility(data):
    # 년도별 분석
    yearly = data.groupBy("자치구 이름", "년도")         .agg(
            F.mean("가격(원)").alias("년도별 평균 가격"),
            F.stddev("가격(원)").alias("표준편차"),
            F.count("*").alias("년도별 품목 수")
        )
    yearly = yearly.withColumn(
        "년도별 변동계수(cv)",
        F.when(col("년도별 평균 가격") != 0, col("표준편차") / col("년도별 평균 가격")).otherwise(0)
    )

    # 월별 분석
    monthly = data.groupBy("자치구 이름", "월")         .agg(
            F.mean("가격(원)").alias("월별 평균 가격"),
            F.stddev("가격(원)").alias("표준편차"),
            F.count("*").alias("월별 품목 수")
        )
    monthly = monthly.withColumn(
        "월별 변동계수(cv)",
        F.when(col("월별 평균 가격") != 0, col("표준편차") / col("월별 평균 가격")).otherwise(0)
    )

    return yearly, monthly

# 전체 자치구별 전체 평균
overall_stats = calculate_overall_gu_statistics(all_data)
print("\n===== 자치구별 전체 평균 가격과 변동계수 =====")
overall_stats.show()

# 년도별/월별 결과
yearly, monthly = calculate_gu_distribution_and_volatility(all_data)
print("\n===== 년도별 평균, 변동계수, 품목수 =====")
yearly.show()

print("\n===== 월별 평균, 변동계수, 품목수 =====")
monthly.show()

# 각 카테고리에 대해서도 같은 분석
for category, df in dataframes.items():
    print("\n===== {} =====".format(category))  # f-string 대신 .format() 사용
    if not df.rdd.isEmpty():  # 비어 있지 않은 경우
        y_stats, m_stats = calculate_gu_distribution_and_volatility(df)
        print("\n년도별 분포, 평균, 변동계수, 품목수:")
        y_stats.show()

        print("\n월별 분포, 평균, 변동계수, 품목수:")
        m_stats.show()
    else:
        print("{} 데이터가 비어 있습니다.".format(category))  # f-string 대신 .format() 사용

# Spark 세션 종료
spark.stop()

