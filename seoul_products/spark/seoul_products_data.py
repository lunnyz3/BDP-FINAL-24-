#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, split
from pyspark.sql.types import StructType, StructField, StringType
from xml.etree import ElementTree
import urllib2

# SparkSession 생성
spark = SparkSession.builder     .appName("Seoul Products Data Preprocessing")     .getOrCreate()

# HDFS 경로 설정
data_dir = "/user/maria_dev/spark/data/utf8"

# 'data' 폴더 안의 파일 이름 리스트
file_names = [
    "data_2023.csv",
    "data_2022.csv",
    "data_2021.csv",
    "data_2020.csv",
    "data_2019.csv",
    "data_2018.csv",
    "data_2013_2017.csv",
]

# 데이터를 담을 리스트
dataframes = []
for file_name in file_names:
    file_path = os.path.join(data_dir, file_name)
    # ',' 구분자 사용하여 CSV 파일 읽기
    df = spark.read.format("csv")         .option("header", "true")         .option("delimiter", ",")         .option("encoding", "UTF-8")         .load(file_path)
    dataframes.append(df)

# 데이터프레임 병합
from functools import reduce
from pyspark.sql import DataFrame
data = reduce(DataFrame.unionAll, dataframes)

# 열 이름 출력
print("데이터프레임의 열 이름:", data.columns)

# 필요한 경우 열 이름 매핑
column_mapping = {
    "_c0": "일련번호",
    "_c1": "시장/마트 번호",
    "_c2": "시장/마트 이름",
    "_c3": "품목 번호",
    "_c4": "품목 이름",
    "_c5": "실판매규격",
    "_c6": "가격(원)",
    "_c7": "년도-월",
    "_c8": "비고",
    "_c9": "시장유형 구분(시장/마트) 코드",
    "_c10": "시장유형 구분(시장/마트) 이름",
    "_c11": "자치구 코드",
    "_c12": "자치구 이름",
    "_c13": "점검일자"
}

for old_col, new_col in column_mapping.items():
    if old_col in data.columns:
        data = data.withColumnRenamed(old_col, new_col)

# 열 이름 재확인
print("수정된 열 이름:", data.columns)

# 문자열 데이터 전처리
for c in data.columns:
    data = data.withColumn(c, when(
        (col(c).isNotNull()) & (col(c).contains('";')),
        split(col(c), '";').getItem(0)
    ).otherwise(col(c)))

# NaN 처리
data = data.replace(["nan", "NaN", ""], None)

# 년도-월 형식 확인
unique_year_months = data.select('년도-월').distinct().collect()
print("고유한 년도-월 값: {}".format([row['년도-월'] for row in unique_year_months]))

# API 정보 설정
base_url = "http://openapi.seoul.go.kr:8088/5050624d5a636f6431303573446e6d4b/xml/ListNecessariesPricesService"
total_count = 97391  # 전체 데이터 개수
page_size = 1000  # 최대 1000개 호출 가능

all_data = []

# 페이지별로 데이터를 호출
for start in range(1, total_count + 1, page_size):
    end = min(start + page_size - 1, total_count)
    url = "{}/{}/{}/".format(base_url, start, end)
    print("현재 호출 중: {}~{}".format(start, end))

    try:
        response = urllib2.urlopen(url)
        api_result = response.read()  # 디코딩하지 않음

        try:
            root = ElementTree.fromstring(api_result)  # XML 파싱

            for row in root.findall(".//row"):
                record = {}
                for child in row:
                    value = child.text
                    if isinstance(value, str):  # 바이트 문자열인 경우
                        value = value.decode('utf-8')  # UTF-8로 디코딩
                    record[child.tag] = value if value else None

                # 날짜 필터링: 2024년 데이터만 추가
                p_date = record.get("P_DATE", "")
                if p_date and p_date.startswith("2024"):
                    all_data.append(record)

        except ElementTree.ParseError as e:
            print("XML 파싱 오류 발생: {}~{}, 오류 내용: {}".format(start, end, e))
            continue

    except urllib2.HTTPError as e:
        print("HTTP 에러 발생: {}~{}, 상태 코드: {}".format(start, end, e.code))
    except urllib2.URLError as e:
        print("URL 에러 발생: {}~{}, 오류 내용: {}".format(start, end, e.reason))

# None 값을 빈 문자열로 대체 (필요 시)
for record in all_data:
    for key in record:
        if record[key] is None:
            record[key] = ''

# 스키마 정의
schema = StructType([
    StructField("P_SEQ", StringType(), True),
    StructField("M_SEQ", StringType(), True),
    StructField("M_NAME", StringType(), True),
    StructField("A_SEQ", StringType(), True),
    StructField("A_NAME", StringType(), True),
    StructField("A_UNIT", StringType(), True),
    StructField("A_PRICE", StringType(), True),
    StructField("P_YEAR_MONTH", StringType(), True),
    StructField("ADD_COL", StringType(), True),
    StructField("P_DATE", StringType(), True),
    StructField("M_TYPE_CODE", StringType(), True),
    StructField("M_TYPE_NAME", StringType(), True),
    StructField("M_GU_CODE", StringType(), True),
    StructField("M_GU_NAME", StringType(), True)
])

# DataFrame 생성
data_2024 = spark.createDataFrame(all_data, schema=schema)

# 컬럼 이름 변경
column_mapping_api = {
    "P_SEQ": "일련번호",
    "M_SEQ": "시장/마트 번호",
    "M_NAME": "시장/마트 이름",
    "A_SEQ": "품목 번호",
    "A_NAME": "품목 이름",
    "A_UNIT": "실판매규격",
    "A_PRICE": "가격(원)",
    "P_YEAR_MONTH": "년도-월",
    "ADD_COL": "비고",
    "P_DATE": "점검일자",
    "M_TYPE_CODE": "시장유형 구분(시장/마트) 코드",
    "M_TYPE_NAME": "시장유형 구분(시장/마트) 이름",
    "M_GU_CODE": "자치구 코드",
    "M_GU_NAME": "자치구 이름"
}

for old_col, new_col in column_mapping_api.items():
    if old_col in data_2024.columns:
        data_2024 = data_2024.withColumnRenamed(old_col, new_col)

# 데이터 타입 통일
data_2024_dtypes = dict(data_2024.dtypes)
data_dtypes = dict(data.dtypes)

for col_name in data_2024_dtypes:
    if col_name in data.columns:
        data_type_2024 = data_2024_dtypes[col_name]
        data_type_data = data_dtypes[col_name]
        if data_type_2024 != data_type_data:
            data = data.withColumn(col_name, data[col_name].cast(data_type_2024))

# 공통 열 추출
common_columns = set(data.columns) & set(data_2024.columns)
common_columns = list(common_columns)

# 공통 열만 선택하여 데이터프레임 재구성
data = data.select(common_columns)
data_2024 = data_2024.select(common_columns)

# 데이터 병합
data_tot = data_2024.unionByName(data)

# 저장 폴더 설정 (HDFS 상의 경로)
save_dir = "/user/maria_dev/data_concatenated"
total_file_path = os.path.join(save_dir, "data_total.csv")

# HDFS 상에 디렉토리 생성 (필요 시)
hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(save_dir)
if not hdfs.exists(path):
    hdfs.mkdirs(path)

# 최종 병합 데이터 csv로 저장
data_tot.coalesce(1).write.format("csv")     .option("header", "true")     .option("encoding", "UTF-8")     .mode("overwrite")     .save(total_file_path)
print("최종 병합 데이터 저장 완료: {}".format(total_file_path))

# 결과 출력
data_tot.show(5)

# 년도-월 형식 확인
unique_year_months_tot = data_tot.select('년도-월').distinct().collect()
print("고유한 년도-월 값: {}".format([row['년도-월'] for row in unique_year_months_tot]))

# SparkSession 종료
spark.stop()

