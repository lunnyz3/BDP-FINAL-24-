#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
reload(sys)
sys.setdefaultencoding('utf8')  # Python 2.7 환경에서 UTF-8 기본 인코딩 설정

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def uprint(s):
    # 유니코드 안전 출력을 위한 함수
    sys.stdout.write(s.encode("utf-8") + "\n")

# Spark 세션 생성
spark = SparkSession.builder     .appName("Item Distribution and Volatility Analysis")     .getOrCreate()

# 데이터 디렉토리(HDFS 경로)
data_dir = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/spark/data2"

# 필터링할 카테고리
filtered_categories = [u"data_agriculture", u"data_livestock", u"data_seafood"]

# 카테고리별 파일 목록
category_files = [
    u"data_agriculture.csv",
    u"data_livestock.csv",
    u"data_seafood.csv"
]

# 아이템 명 매핑
item_to_variable = {
    u"양파": u"onion",
    u"오이": u"cucumber",
    u"배추": u"cabbage",
    u"돼지고기": u"pork",
    u"닭고기": u"chicken",
    u"오징어": u"squid",
    u"명태": u"pollack",
    u"고등어": u"mackerel"
}

dataframes = {}
for file_name in category_files:
    category_name = file_name.replace(u".csv", u"")
    file_path = u"{}/{}".format(data_dir, file_name)
    try:
        df = spark.read.option("header", "true")             .option("encoding", "UTF-8")             .csv(file_path)
        if df.columns:
            # 컬럼명 공백 제거
            df = df.toDF(*[c.strip() for c in df.columns])
            if u"가격(원)" in df.columns:
                df = df.withColumn(u"가격(원)", col(u"가격(원)").cast("double"))
            dataframes[category_name] = df
        else:
            dataframes[category_name] = None
    except Exception as e:
        uprint(u"{} 파일을 읽는 중 오류 발생: {}".format(file_name, e))
        dataframes[category_name] = None

separated_data = {}
for item_name, variable_name in item_to_variable.items():
    filtered_parts = []
    for category in filtered_categories:
        df = dataframes.get(category, None)
        if df is not None and not df.rdd.isEmpty() and u"품목 이름" in df.columns:
            part = df.filter(col(u"품목 이름").contains(item_name))
            filtered_parts.append(part)
    if filtered_parts:
        combined_df = filtered_parts[0]
        for p in filtered_parts[1:]:
            combined_df = combined_df.union(p)
        separated_data[variable_name] = combined_df
    else:
        separated_data[variable_name] = None

# 오이 데이터 처리
cucumber = separated_data.get(u"cucumber", None)
if cucumber is not None and not cucumber.rdd.isEmpty():
    if u"품목 이름" in cucumber.columns and u"가격(원)" in cucumber.columns:
        cucumber = cucumber.withColumn(
            u"가격(원)",
            when(col(u"품목 이름") == u"오이 1개", col(u"가격(원)")/2).otherwise(col(u"가격(원)"))
        )
        if u"실판매규격" in cucumber.columns:
            cucumber = cucumber.withColumn(
                u"실판매규격",
                when(col(u"품목 이름") == u"오이 1개", u"1개").otherwise(col(u"실판매규격"))
            )
        separated_data[u"cucumber"] = cucumber

def remove_outliers_iqr(df, column):
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
    if len(quantiles) < 2:
        return df, 0
    Q1, Q3 = quantiles
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    before_count = df.count()
    filtered_df = df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))
    after_count = filtered_df.count()
    removed_count = before_count - after_count
    return filtered_df, removed_count

dataframes_to_process = [u"onion", u"cucumber", u"cabbage", u"pork", u"chicken", u"squid", u"pollack", u"mackerel"]
removed_counts = {}
processed_data = {}

for df_name in dataframes_to_process:
    df = separated_data.get(df_name, None)
    if df is not None and not df.rdd.isEmpty() and u"가격(원)" in df.columns:
        filtered_df, removed_count = remove_outliers_iqr(df, u"가격(원)")
        processed_data[df_name] = filtered_df
        removed_counts[df_name] = removed_count
    else:
        processed_data[df_name] = df

uprint(u"\n\n=== 이상치 제거 결과 ===")
for df_name, count in removed_counts.items():
    uprint(u"{}: Removed {} rows".format(df_name, count))

def calculate_distribution_and_volatility(df):
    if df is None or df.rdd.isEmpty():
        return None, None

    if not (u"년도" in df.columns and u"월" in df.columns and u"가격(원)" in df.columns):
        return None, None

    yearly = df.groupBy(u"년도").agg(
        F.mean(u"가격(원)").alias(u"년도별 평균 가격"),
        F.stddev(u"가격(원)").alias("std"),
        F.count("*").alias(u"년도별 품목 수")
    )
    yearly = yearly.withColumn(u"년도별 변동계수(cv)", F.when(col(u"년도별 평균 가격") != 0, col("std")/col(u"년도별 평균 가격")).otherwise(0)).drop("std")

    monthly = df.groupBy(u"월").agg(
        F.mean(u"가격(원)").alias(u"월별 평균 가격"),
        F.stddev(u"가격(원)").alias("std"),
        F.count("*").alias(u"월별 품목 수")
    )
    monthly = monthly.withColumn(u"월별 변동계수(cv)", F.when(col(u"월별 평균 가격") != 0, col("std")/col(u"월별 평균 가격")).otherwise(0)).drop("std")

    return yearly, monthly

uprint(u"\n------------- 아이템별 년도/월별 분석 및 결과 저장 -------------")

output_base = u"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/spark/output"

def save_results(df, path, desc):
    if df is not None and not df.rdd.isEmpty():
        df.write.mode("overwrite").option("header","true").csv(path)
        uprint(u"{} 저장 완료: {}".format(desc, path))
    else:
        uprint(u"{} 데이터가 비어 있거나 저장 대상 없음.".format(desc))

for name, df in processed_data.items():
    uprint(u"===== {} =====".format(name))
    if df is not None and not df.rdd.isEmpty() and u"년도" in df.columns and u"월" in df.columns and u"가격(원)" in df.columns:
        yearly, monthly = calculate_distribution_and_volatility(df)

        # take() 사용하여 로컬로 가져온 뒤 uprint로 안전하게 출력
        uprint(u"년도별 데이터 일부:")
        for row in yearly.take(10):
            uprint(u",".join([unicode(x) for x in row]))

        uprint(u"월별 데이터 일부:")
        for row in monthly.take(10):
            uprint(u",".join([unicode(x) for x in row]))

        # 결과 저장
        yearly_path = u"{}/{}_yearly".format(output_base, name)
        monthly_path = u"{}/{}_monthly".format(output_base, name)

        save_results(yearly, yearly_path, u"{} 년도별 결과".format(name))
        save_results(monthly, monthly_path, u"{} 월별 결과".format(name))
    else:
        uprint(u"{} 데이터가 비어 있거나 필요한 컬럼이 없습니다.".format(name))
    uprint(u"\n")

# Spark 세션 종료
spark.stop()


