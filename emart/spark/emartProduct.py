from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Filter Products in HDFS") \
    .getOrCreate()

# HDFS에 저장된 CSV 파일 경로
file_path = "hdfs:///user/maria_dev/csv_data/products.csv"

# CSV 파일 읽기
data = spark.read.csv(file_path, header=True, inferSchema=True)

# 포함 및 제외 키워드 리스트
target_products = ['명태', '고등어', '오징어', '양파', '오이', '배추', '돼지고기', '닭고기']
exclude_keywords = ['맛', '가공', '조미', '채', '깡', '튀김']

# UDF를 사용하여 필터링 로직 정의
def is_valid_title(title):
    if not title:  # 제목이 None이거나 비어있는 경우 제외
        return False
    # 제외 키워드가 포함된 경우 False
    if any(exclude in title for exclude in exclude_keywords):
        return False
    # 대상 품목 키워드가 포함된 경우 True
    return any(target in title for target in target_products)

# UDF 등록
is_valid_title_udf = udf(is_valid_title, BooleanType())

# 필터링 적용
filtered_data = data.filter(is_valid_title_udf(col("Title")))

# 결과 출력
filtered_data.show(truncate=False)

# 필터링된 데이터를 HDFS에 저장
output_path = "hdfs:///user/maria_dev/csv_data/filtered_products.csv"
filtered_data.write.csv(output_path, header=True, mode="overwrite")

print(f"필터링된 데이터를 '{output_path}' 경로에 저장했습니다.")
