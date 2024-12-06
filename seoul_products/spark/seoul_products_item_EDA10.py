#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# SparkSession 생성
spark = SparkSession.builder     .appName("Seoul Products Data EDA")     .config("spark.executor.memory", "2g")     .config("spark.driver.memory", "2g")     .getOrCreate()

# 데이터 로드
file_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/spark/data_total.csv"
data_tot = spark.read.option("header", "true").option("encoding", "UTF-8").csv(file_path)

# 열 이름 매핑
column_mapping = {
    "일련번호": "id",
    "시장/마트 번호": "market_id",
    "시장/마트 이름": "market_name",
    "품목 번호": "item_id",
    "품목 이름": "item_name",
    "실판매규격": "sales_unit",
    "가격(원)": "price",
    "년도-월": "year_month",
    "비고": "remark",
    "점검일자": "inspection_date",
    "시장유형 구분(시장/마트) 코드": "market_type_code",
    "시장유형 구분(시장/마트) 이름": "market_type_name",
    "자치구 코드": "district_code",
    "자치구 이름": "district_name",
}

for old_name, new_name in column_mapping.items():
    data_tot = data_tot.withColumnRenamed(old_name, new_name)

# price 열을 숫자형으로 변환
data_tot = data_tot.withColumn("price", col("price").cast("double"))

# 0원 비율 계산
zero_count_by_item = data_tot.filter(col('price') == 0).groupby('item_name').agg(F.count('*').alias('zero_count'))
total_count_by_item = data_tot.groupby('item_name').agg(F.count('*').alias('total_count'))

# Join 후 zero_ratio 계산
zero_ratio = zero_count_by_item.join(total_count_by_item, "item_name")     .withColumn("zero_ratio", (col("zero_count") / col("total_count")) * 100)

# 0원 비율이 30% 이하인 품목 처리
low_zero_ratio_items = zero_ratio.filter(col("zero_ratio") <= 30).select("item_name").distinct()

# 연도별 중앙값 계산
data_with_medians = (
    data_tot.filter(data_tot.item_name.isin([row.item_name for row in low_zero_ratio_items.collect()]))
    .groupBy("item_name", "year_month")
    .agg(F.expr("percentile_approx(price, 0.5)").alias("median_price"))
)

# 데이터 병합 시 컬럼 이름 명시
data_tot = data_tot.alias("tot").join(
    data_with_medians.alias("med"),
    (col("tot.item_name") == col("med.item_name")) & (col("tot.year_month") == col("med.year_month")),
    "left"
).drop(col("med.item_name")).drop(col("med.year_month"))

# 중앙값으로 0원 대체
data_tot = data_tot.withColumn(
    "price",
    F.when(col("price") == 0, col("median_price")).otherwise(col("price"))
).drop("median_price")

# 0원 비율이 30% 초과인 품목 제거
high_zero_ratio_items = zero_ratio.filter(col("zero_ratio") > 30).select("item_name").distinct()
data_tot_filtered = data_tot.filter(~data_tot.item_name.isin([row.item_name for row in high_zero_ratio_items.collect()]))

# 최종 데이터 확인 및 저장
data_tot_filtered.show()

# 데이터 저장
save_dir = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/spark/data_processed"
data_tot_filtered.write.csv(save_dir, mode="overwrite", header=True)
print("최종 데이터 저장 완료:", save_dir)

