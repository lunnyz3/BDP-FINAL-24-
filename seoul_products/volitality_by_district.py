# --------- 자치구별로 월/년도별 평균과 변동 계수 ----------------
print("----------------- 자치구별로 월/년도별 평균과 변동 계수 ----------------")

# -*- coding: utf-8 -*-
import os
import pandas as pd

# 파일 경로 설정
data_dir = os.path.join(os.getcwd(), "data_categorized")

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
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        dataframes[file_name.replace(".csv", "")] = pd.read_csv(file_path, encoding="utf-8-sig")
    else:
        print(f"{file_name} 파일을 찾을 수 없습니다. 빈 데이터로 처리합니다.")
        dataframes[file_name.replace(".csv", "")] = pd.DataFrame()


# 자치구별 전체 평균, 변동계수 및 데이터 개수 계산 함수
def calculate_overall_gu_statistics(data):
    overall = data.groupby('자치구 이름')["가격(원)"].agg(['mean', 'std', 'count']).reset_index()
    overall['cv'] = overall['std'] / overall['mean']  # 변동계수 계산
    overall.rename(columns={
        'mean': '전체 평균 가격',
        'std': '표준편차',
        'count': '데이터 개수',
        'cv': '전체 변동계수(cv)'
    }, inplace=True)
    return overall[['자치구 이름', '전체 평균 가격', '전체 변동계수(cv)', '데이터 개수']]


# 자치구별 년도별 및 월별 분석 함수
def calculate_gu_distribution_and_volatility(data):
    # 년도별 분석
    yearly = data.groupby(['자치구 이름', '년도'])["가격(원)"].agg(['mean', 'std', 'count']).reset_index()
    yearly['cv'] = yearly['std'] / yearly['mean']  # 변동계수 계산
    yearly.rename(columns={
        'mean': '년도별 평균 가격',
        'cv': '년도별 변동계수(cv)',
        'count': '년도별 품목 수'
    }, inplace=True)

    # 월별 분석
    monthly = data.groupby(['자치구 이름', '월'])["가격(원)"].agg(['mean', 'std', 'count']).reset_index()
    monthly['cv'] = monthly['std'] / monthly['mean']  # 변동계수 계산
    monthly.rename(columns={
        'mean': '월별 평균 가격',
        'cv': '월별 변동계수(cv)',
        'count': '월별 품목 수'
    }, inplace=True)

    return yearly, monthly


# 전체 자치구별 분석
print("------------- 자치구별 전체 평균 가격과 변동계수 -------------")
all_data = pd.concat(dataframes.values(), ignore_index=True)
overall_stats = calculate_overall_gu_statistics(all_data)
print("\n===== 자치구별 전체 평균 가격과 변동계수 =====")
print(overall_stats.round(2))

# 년도별 및 월별 분석
yearly, monthly = calculate_gu_distribution_and_volatility(all_data)

print("\n===== 년도별 평균, 변동계수, 품목수 =====")
print(yearly.round(2))

print("\n===== 월별 평균, 변동계수, 품목수 =====")
print(monthly.round(2))


# 각 카테고리에 대해 분석 및 출력
print("\n------------- 카테고리별 + 자치구별 전체 평균 가격과 변동계수 -------------")
for category, df in dataframes.items():
    print(f"\n===== {category} =====")
    if not df.empty:  # 데이터프레임이 비어있지 않을 경우만 계산
        yearly, monthly = calculate_gu_distribution_and_volatility(df)
        
        # 년도별 결과 출력
        print("\n년도별 분포, 평균, 변동계수, 품목수:")
        print(yearly.round(2))

        # 월별 결과 출력
        print("\n월별 분포, 평균, 변동계수, 품목수:")
        print(monthly.round(2))
    else:
        print(f"{category} 데이터가 비어 있습니다.")
