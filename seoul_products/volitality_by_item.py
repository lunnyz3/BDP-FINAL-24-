import os
import pandas as pd

# 전처리한 데이터 불러오기
folder_path = "/home/maria_dev/spark/data_categorized"

# 데이터 카테고리 지정
filtered_categories = ["data_agriculture", "data_livestock", "data_seafood"]

# 결과 저장용
filtered_dataframes = {}

# 데이터 로드
for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        category_name = os.path.splitext(file_name)[0]
        if category_name in filtered_categories:
            file_path = os.path.join(folder_path, file_name)
            filtered_dataframes[category_name] = pd.read_csv(file_path, encoding="utf-8-sig")

# target_items에 해당하는 품목 이름을 영어로 변수에 매핑
item_to_variable = {
    "양파": "onion",
    "오이": "cucumber",
    "배추": "cabbage",
    "돼지고기": "pork",
    "닭고기": "chicken",
    "오징어": "squid",
    "명태": "pollack",
    "고등어": "mackerel"
}

# 결과를 저장할 딕셔너리
separated_data = {}

# 데이터 처리
for item_name, variable_name in item_to_variable.items():
    filtered_rows = []
    for category in ["data_agriculture", "data_livestock", "data_seafood"]:
        if category in filtered_dataframes:
            df = filtered_dataframes[category]
            # 품목 이름에 해당 단어가 포함된 행 추출
            if "품목 이름" in df.columns:
                filtered_rows.append(df[df["품목 이름"].str.contains(item_name, na=False)])
    # 하나의 데이터프레임으로 합치기
    if filtered_rows:
        separated_data[variable_name] = pd.concat(filtered_rows, ignore_index=True)

# 결과 변수에 데이터 저장
onion = separated_data.get("onion", pd.DataFrame())
cucumber = separated_data.get("cucumber", pd.DataFrame())
cabbage = separated_data.get("cabbage", pd.DataFrame())
pork = separated_data.get("pork", pd.DataFrame())
chicken = separated_data.get("chicken", pd.DataFrame())
squid = separated_data.get("squid", pd.DataFrame())
pollack = separated_data.get("pollack", pd.DataFrame())
mackerel = separated_data.get("mackerel", pd.DataFrame())

# 결과 확인
for var_name, data in separated_data.items():
    print(f"Data for {var_name} (Total rows: {len(data)})")
    print(f"품목 이름: {data['품목 이름'].unique()}")
    print(data.head(), "\n")

print("\n\n")
# -------------- 'cucumber'에서 '품목 이름'이 '오이 1개'인 데이터 수정 --------------
if 'cucumber' in separated_data:
    cucumber = separated_data['cucumber']

    # '품목 이름'이 '오이 1개'인 행 필터링
    cucumber.loc[cucumber['품목 이름'] == '오이 1개', '가격(원)'] = cucumber.loc[cucumber['품목 이름'] == '오이 1개', '가격(원)'] / 2
    
    # '실판매규격'을 '1개'로 업데이트
    cucumber.loc[cucumber['품목 이름'] == '오이 1개', '실판매규격'] = '1개'

    # 결과 확인
    print(f"Updated rows for '오이 1개':\n{cucumber[cucumber['품목 이름'] == '오이 1개']}")


# -------------------- item마다 이상치 제거 -----------------------

def remove_outliers_iqr(df, column):
    """
    IQR 방식으로 이상치를 제거하는 함수
    """
    Q1 = df[column].quantile(0.25)  # 1분위수
    Q3 = df[column].quantile(0.75)  # 3분위수
    IQR = Q3 - Q1  # IQR 계산
    lower_bound = Q1 - 1.5 * IQR  # 하한값
    upper_bound = Q3 + 1.5 * IQR  # 상한값
    filtered_df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    removed_count = len(df) - len(filtered_df)  # 제거된 행의 개수 계산
    return filtered_df, removed_count

# 대상 데이터프레임 목록
dataframes_to_process = [onion, cucumber, cabbage, pork, chicken, squid, pollack, mackerel]

# 결과 저장
removed_counts = {}
processed_data = {}

# 이상치 제거 수행
for df_name, df in zip(["onion", "cucumber", "cabbage", "pork", "chicken", "squid", "pollack", "mackerel"], dataframes_to_process):
    if not df.empty and "가격(원)" in df.columns:
        filtered_df, removed_count = remove_outliers_iqr(df, "가격(원)")
        processed_data[df_name] = filtered_df
        removed_counts[df_name] = removed_count

# 결과 출력
print("\n\n=== 이상치 제거 결과 ===")
for df_name, removed_count in removed_counts.items():
    print(f"{df_name}: Removed {removed_count} rows")


# ------------------------ item별 평균 가격과 변동성 계산 ---------------------
# 함수: 년도 및 월별 분포, 평균, 변동계수 계산
def calculate_distribution_and_volatility(data):
    # 년도별 분포, 평균, 변동계수 계산
    yearly = data.groupby('년도')["가격(원)"].agg(['mean', 'std', 'count']).reset_index()
    yearly['cv'] = yearly['std'] / yearly['mean']  # 변동계수 계산
    yearly.rename(columns={'mean': '년도별 평균 가격', 'cv': '년도별 변동계수(cv)', 'count': '년도별 품목 수'}, inplace=True)
    
    # 월별 분포, 평균, 변동계수 계산
    monthly = data.groupby('월')["가격(원)"].agg(['mean', 'std', 'count']).reset_index()
    monthly['cv'] = monthly['std'] / monthly['mean']  # 변동계수 계산
    monthly.rename(columns={'mean': '월별 평균 가격', 'cv': '월별 변동계수(cv)', 'count': '월별 품목 수'}, inplace=True)
    
    return yearly, monthly

# 모든 카테고리에 대해 년도 및 월별 분석 및 출력
for name, df in processed_data.items():  # 여기서 processed_data 사용
    print(f"===== {name} =====")
    if not df.empty:  # 데이터프레임이 비어있지 않을 경우만 계산
        yearly, monthly = calculate_distribution_and_volatility(df)
        print("년도별 분포, 평균, 변동계수:")
        print(yearly.round(2))
        print("\n월별 분포, 평균, 변동계수:")
        print(monthly.round(2))
    else:
        print(f"{name} 데이터가 비어 있습니다.")
    print("\n")


# ---------------- 데이터 저장 ---------------------
# 데이터 저장 경로 설정
output_folder = os.path.join(os.getcwd(), "data_items")
os.makedirs(output_folder, exist_ok=True)  # 디렉토리가 없으면 생성

# processed_data를 각각 CSV 파일로 저장
for item_name, df in processed_data.items():
    if not df.empty:  # 데이터프레임이 비어있지 않을 경우만 저장
        file_path = os.path.join(output_folder, f"{item_name}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"{item_name} 데이터 저장 완료: {file_path}")
    else:
        print(f"{item_name} 데이터가 비어 있어 저장되지 않았습니다.")