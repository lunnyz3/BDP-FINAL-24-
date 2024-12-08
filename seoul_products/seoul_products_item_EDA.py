import os
import pandas as pd
from collections import defaultdict

current_dir = os.getcwd()
# 전처리한 data들 불러오기
folder_path = "/home/maria_dev/spark/data_categorized"
# 대표 카테고리 3개 선정
filtered_categories = ["data_agriculture", "data_livestock", "data_seafood"]
filtered_dataframes = {}

for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        category_name = os.path.splitext(file_name)[0]
        if category_name in filtered_categories:
            file_path = os.path.join(folder_path, file_name)
            filtered_dataframes[category_name] = pd.read_csv(file_path, encoding="utf-8-sig")

all_items = defaultdict(set)
for category in filtered_categories:
    if category in filtered_dataframes:
        df = filtered_dataframes[category]
        if "품목 이름" in df.columns:  # "품목 이름" 컬럼 존재 여부 확인
            unique_items = df["품목 이름"].unique()
            all_items[category].update(unique_items)

# ----- 각 데이터 카테고리에 어떤 품목들이 있는지 확인 -----
for category, items in all_items.items():
    print(f"=== {category} ===")
    for item in sorted(items):
        print(item)
    print("\n")

# 2013-2024년까지 다 있는 품목들 확인
organized_results = defaultdict(lambda: defaultdict(set))

for category, df in filtered_dataframes.items():
    if "품목 이름" in df.columns and "년도" in df.columns:
        for _, row in df.iterrows():
            item = row["품목 이름"]
            year = row["년도"]
            organized_results[category][item].add(year)

# 결과 출력 (지정된 순서)
print("------------ 각 데이터 카테고리에 포함된 품목 및 연도 ------------\n")
for category in ["data_agriculture", "data_livestock", "data_seafood"]:
    if category in organized_results:
        print(f"=== {category} ===")
        for item, years in sorted(organized_results[category].items()):
            print(f"{item}: {sorted(years)}")
        print("\n")



# ----------- 사용할 품목들 리스트(2024년까지 꾸준히 있던 data) -------------

target_items = {"양파", "애호박", "무", "상추", "오이", "사과", "배", "배추", 
                "돼지고기", "닭고기", "조기", "오징어", "명태", "고등어"}
comparison_results = defaultdict(list)

# 데이터 처리 및 결과 저장 (지정된 순서로 처리)
for category in ["data_agriculture", "data_livestock", "data_seafood"]:
    if category in filtered_dataframes:
        df = filtered_dataframes[category]
        if "품목 이름" in df.columns and "가격(원)" in df.columns and "실판매규격" in df.columns:
            # target_items에 포함된 품목만 필터링
            # target_items와 일치하거나 포함되는 품목 필터링
            filtered_df = df[df["품목 이름"].str.contains("|".join(target_items), na=False)]


            for item, group in filtered_df.groupby("품목 이름"):
                avg_price = group["가격(원)"].mean()  # 평균 가격 계산
                count = len(group)  # 데이터 개수
                spec = group["실판매규격"].mode()[0] if not group["실판매규격"].mode().empty else "nan"  # 실판매규격
                etc = group["비고"].mode()[0] if not group["비고"].mode().empty else "nan"
                comparison_results[category].append({
                    "품목 이름": item,
                    "실판매규격": spec,
                    "비고": etc,
                    "평균 가격(원)": avg_price,
                    "데이터 개수": count
                })

# 결과 출력 (지정된 순서로)
for category in ["data_agriculture", "data_livestock", "data_seafood"]:
    print(f"=== {category} ===")
    for item in comparison_results[category]:
        print(
            f"품목 이름: {item['품목 이름']}, "
            f"실판매규격: {item['실판매규격']}, "
            f"비고: {item['비고']}, "
            f"평균 가격(원): {item['평균 가격(원)']:.2f}, "
            f"데이터 개수: {item['데이터 개수']}"
        )
    print("\n")