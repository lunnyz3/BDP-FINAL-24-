#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import sys

def uprint(s):
    # Python 2.7에서 유니코드 문자열 안전 출력
    sys.stdout.write(s.encode("utf-8") + "\n")

# Spark 세션 생성
spark = SparkSession.builder     .appName("Gu Distribution Analysis")     .getOrCreate()

# 데이터 디렉토리 설정 (HDFS 경로)
data_dir = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/spark/data2"

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
    file_path = u"{}/{}".format(data_dir, file_name)
    try:
        # HDFS 상의 CSV 파일 읽기
        # header 옵션: 첫 줄을 헤더로 간주
        # encoding 옵션: UTF-8 설정
        df = spark.read.option("header", "true")             .option("encoding", "UTF-8")             .csv(file_path)

        dataframes[file_name.replace(u".csv", u"")] = df
    except Exception as e:
        print("Error reading {}: {}".format(file_name, e))
        # 빈 데이터프레임 생성 시 스키마 없이 생성 불가하므로 기본 스키마 지정 필요할 수도 있으나
        # 여기서는 빈 DataFrame을 None으로 처리
        dataframes[file_name.replace(u".csv", u"")] = None

# 모든 데이터 합치기
all_data = None
for df in dataframes.values():
    if df is not None:
        all_data = df if all_data is None else all_data.union(df)

# '가격(원)'을 숫자형으로 변환
if all_data is not None:
    all_data = all_data.withColumn(u"가격(원)", col(u"가격(원)").cast("double"))
else:
    # 모든 데이터가 비었을 경우 처리
    uprint(u"모든 데이터가 비어 있습니다.")
    spark.stop()
    sys.exit(0)

# 전체 자치구별 평균, 변동계수 계산 함수
def calculate_overall_gu_statistics(data):
    stats = data.groupBy(u"자치구 이름")         .agg(
            F.mean(u"가격(원)").alias(u"전체 평균 가격"),
            F.stddev(u"가격(원)").alias(u"표준편차"),
            F.count("*").alias(u"데이터 개수")
        )
    stats = stats.withColumn(
        u"전체 변동계수(cv)",
        F.when(col(u"전체 평균 가격") != 0, col(u"표준편차") / col(u"전체 평균 가격")).otherwise(0)
    )
    return stats

# 년도/월별 통계 계산 함수
def calculate_gu_distribution_and_volatility(data):
    # 년도별 분석
    yearly = data.groupBy(u"자치구 이름", u"년도")         .agg(
            F.mean(u"가격(원)").alias(u"년도별 평균 가격"),
            F.stddev(u"가격(원)").alias(u"표준편차"),
            F.count("*").alias(u"년도별 품목 수")
        )
    yearly = yearly.withColumn(
        u"년도별 변동계수(cv)",
        F.when(col(u"년도별 평균 가격") != 0, col(u"표준편차") / col(u"년도별 평균 가격")).otherwise(0)
    )

    # 월별 분석
    monthly = data.groupBy(u"자치구 이름", u"월")         .agg(
            F.mean(u"가격(원)").alias(u"월별 평균 가격"),
            F.stddev(u"가격(원)").alias(u"표준편차"),
            F.count("*").alias(u"월별 품목 수")
        )
    monthly = monthly.withColumn(
        u"월별 변동계수(cv)",
        F.when(col(u"월별 평균 가격") != 0, col(u"표준편차") / col(u"월별 평균 가격")).otherwise(0)
    )

    return yearly, monthly

uprint(u"\n===== 자치구별 전체 평균 가격과 변동계수 =====")
overall_stats = calculate_overall_gu_statistics(all_data)
overall_stats.show()

uprint(u"\n===== 년도별 평균, 변동계수, 품목수 =====")
yearly, monthly = calculate_gu_distribution_and_volatility(all_data)
yearly.show()

uprint(u"\n===== 월별 평균, 변동계수, 품목수 =====")
monthly.show()

# 각 카테고리에 대해서도 같은 분석
uprint(u"\n------------- 카테고리별 월/년도별 분석 -------------")
for category, df in dataframes.items():
    uprint(u"\n===== {} =====".format(category))
    if df is not None and not df.rdd.isEmpty():
        y_stats, m_stats = calculate_gu_distribution_and_volatility(df)
        uprint(u"\n년도별 분포, 평균, 변동계수, 품목수:")
        y_stats.show()

        uprint(u"\n월별 분포, 평균, 변동계수, 품목수:")
        m_stats.show()
    else:
        uprint(u"{} 데이터가 비어 있습니다.".format(category))

# Spark 세션 종료
spark.stop()


