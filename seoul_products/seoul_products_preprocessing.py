#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd

# 파일 경로 설정
file_path = os.path.join(os.getcwd(), "data_concatenated", "data_total.csv")

# CSV 파일 읽기
data_tot = pd.read_csv(file_path, encoding="utf-8-sig")

# -------------------------- '년도-월' 전처리 --------------------------
print("------------'년도-월' 전처리-------------")
# '년도-월'에서 NaN 값 제거
print("'년도-월' NAN 제거 전: ", data_tot.shape)
data_tot = data_tot.dropna(subset=['년도-월'])
print("'년도-월' NAN 제거 후: ", data_tot.shape)


# '년도-월' 열에서 연도와 월 분리
data_tot['년도'] = data_tot['년도-월'].astype(str).str[:4]  # 연도 추출
data_tot['월'] = data_tot['년도-월'].astype(str).str[5:7]   # 월 추출


# '년도'와 '월'의 고유값 추출
unique_years = data_tot['년도'].unique()
unique_months = data_tot['월'].unique()

# 결과 출력
print("unique years: ", unique_years)
print("unique months: ", unique_months)

# ----- 0.3월을 3월로 대체 -----

print('\n==== 0.3월을 3월로 처리 ====')
# '월' 값에서 숫자로 변환할 수 없는 값 처리
data_tot['월'] = pd.to_numeric(data_tot['월'], errors='coerce')  # 숫자가 아닌 값은 NaN으로 변경

# NaN 값 제거 또는 대체 (필요 시 선택)
data_tot['월'] = data_tot['월'].fillna(0)  # NaN 값을 0으로 대체 (필요에 따라 다른 값으로 대체 가능)

# '월' 값이 0.3인 데이터를 3으로 변경
data_tot['월'] = data_tot['월'].replace(0.3, 3)

# '월' 열의 값을 정수형으로 변환
data_tot['월'] = data_tot['월'].astype(int)

# 변환 후 데이터 확인
print(data_tot['월'].unique())

# '년도' 열을 숫자형으로 변환
print("\n==== '년도'를 숫자로 변환 ====")
data_tot['년도'] = pd.to_numeric(data_tot['년도'], errors='coerce')
print(data_tot['년도'].dtype)

# # -------------------------- 나머지 column 전처리 --------------------------

# 여러 컬럼 제거 (원본 수정)
columns_to_drop = ['일련번호', '시장/마트 번호', '점검일자', '시장유형 구분(시장/마트) 코드', '자치구 코드']
data_tot.drop(columns=columns_to_drop, inplace=True)

# 제거 후 데이터 확인
print("\n\n------------ 필요 없는 column 제거 ------------")
print(data_tot.head())


# '품목 이름'이 null인 데이터 제거
data_tot = data_tot.dropna(subset=['품목 이름'])
print("'품목 이름' null 개수", data_tot.isnull().sum())


# -------------- column type 변환 ------------------
data_tot["가격(원)"] = pd.to_numeric(data_tot["가격(원)"], errors="coerce")
print("'가격' dtype: ", data_tot["가격(원)"].dtype)


# ----------- 가격 0원 median으로 대체 (0원 비율이 30% 이하인 품목만) ----------------

# '가격(원)' 열에서 NaN 값 개수
nan_count = data_tot['가격(원)'].isna().sum()

# '가격(원)' 열에서 0인 값 개수
zero_count = (data_tot['가격(원)'] == 0).sum()

print("\n\n----------- 가격 0원을 median으로 replace -----------")
print("가격 nan_count: ", nan_count)
print("가격 zero_count: ",zero_count)

zero_data = data_tot[data_tot['가격(원)'] == 0]
print("가격 0인 '품목 이름': ", zero_data['품목 이름'].unique)


pd.set_option('display.max_rows', None)  # 모든 행 출력

# '가격(원)'이 0인 경우의 개수 계산
zero_count_by_item = data_tot[data_tot['가격(원)'] == 0].groupby('품목 이름')['가격(원)'].count()

# 원래 데이터 개수 계산
total_count_by_item = data_tot.groupby('품목 이름')['가격(원)'].count()

# 두 데이터 병합
comparison = pd.DataFrame({
    '전체 데이터 개수': total_count_by_item,
    '0원 개수': zero_count_by_item
}).fillna(0).astype(int)  # NaN 값은 0으로 채우고 정수형 변환

# 정렬하여 확인 (0원 개수 기준 내림차순)
comparison = comparison.sort_values(by='0원 개수', ascending=False)

# 결과 출력
print("==== 전체 데이터 개수와 0원 개수 비교 ====")
print(comparison)

# 0원 비율 계산
zero_ratio = (zero_count_by_item / total_count_by_item).fillna(0) * 100

# 결과 병합
comparison = pd.DataFrame({
    '전체 데이터 개수': total_count_by_item,
    '0원 개수': zero_count_by_item,
    '0원 비율(%)': zero_ratio
}).fillna(0).sort_values(by='0원 비율(%)', ascending=False)

# 0원 비율이 30% 이하인 품목 확인
low_zero_ratio_items = comparison[(comparison['0원 비율(%)'] <= 30) & (comparison['0원 비율(%)'] > 0)]

# 결과 출력
print("\n==== 0원 비율 30% 이하 품목 ====")
print(low_zero_ratio_items)


# 전체 데이터 개수 대비 0원 비율이 30% 이하인 것만 해당 년도의 중앙값으로 대체
data_tot_2 = data_tot.copy()

# 0원 비율 30% 이하 품목 추출
valid_items = low_zero_ratio_items.index

# 대체 작업 진행
for item in valid_items:
    # 해당 품목의 데이터
    item_data = data_tot[data_tot['품목 이름'] == item]

    # 연도별 중앙값 계산
    yearly_medians = item_data.groupby('년도')['가격(원)'].median()

    # 0원 값 대체
    for year, median_price in yearly_medians.items():
        mask = (data_tot['품목 이름'] == item) & (data_tot['년도'] == year) & (data_tot['가격(원)'] == 0)
        data_tot.loc[mask, '가격(원)'] = median_price

# 결과 확인
print("==== 0원 값 대체 완료 ====")


# 0원 비율이 30%를 초과하는 품목 추출
high_zero_ratio_items = comparison[comparison['0원 비율(%)'] > 30].index

# 30%를 초과하는 품목 제거
data_tot = data_tot[~data_tot['품목 이름'].isin(high_zero_ratio_items)]

# 결과 확인
print("\n==== 30% 초과 품목 제거 완료 ====")
print(f"제거된 품목 개수: {len(high_zero_ratio_items)}")
print(f"남은 데이터 개수: {data_tot.shape[0]}")

# 제거된 품목 목록 확인
print("\n==== 제거된 품목 목록 ====")
print(high_zero_ratio_items)


# -------------- 카테고리 그룹화 ------------------
categories = {
    "농산물": [
        "파프리카", "토마토", "콩나물", "콩", "쪽파", "열무", "양파", "양배추", "애호박",
        "쌀", "시금치", "수박", "대파", "당근", "감자", "갓", "고구마", "미나리", "무",
        "상추", "오이", "가지", "브로콜리", "버섯", "깻잎", "사과", "배", "참외", "복숭아", "딸기",
        "포도", "귤", "단감", "오렌지", "생강", "부추", "배추", "바나나", "두부", "도라지", "마늘",
        "풋고추", "고춧가루", "호박", "감", "파"
    ],
    "축산물": ["돼지고기", "소고기", "닭고기", "계란", "삼겹살", "쇠고기", "달걀", "달걀(중란)", "달걀(특란)"],
    "생선/해산물": [
        "조기", "조개", "전복", "새우", "오징어", "갈치", "명태", "꽃게", "굴", "낙지",
        "마른멸치", "동태", "냉동참조기", "고등어", "멸치"
    ],
    "젓갈/장/소스": [
        "식용유", "식초", "설탕", "소금", "케찹", "마요네즈", "참기름", "된장", "간장", "고추장",
        "새우젓", "멸치액젓"
    ],
    "가공식품": [
        "라면", "즉석밥", "통조림", "컵라면", "만두", "부침가루", "밀가루", "국수",
        "햄", "맛김", "어묵", "소시지", "빵"
    ],
    "음료": ["콜라", "사이다", "소주", "맥주", "생수"],
    "유제품": ["우유", "치즈"],
    "기타": []
}
    # 생필품은 0원으로 수집된 데이터가 30% 초과해서 모든 품목 삭제됨

# 카테고리 할당 함수
def clean_item_name(item):
    if isinstance(item, str):
        return item.split("(")[0].strip()  # 괄호와 공백 제거
    return item

def assign_category(item):
    item = clean_item_name(item)  # 품목 이름 정리
    for category, keywords in categories.items():
        if any(keyword in item for keyword in keywords):  # 키워드 포함 여부 확인
            return category
    return "기타"

# 데이터프레임에 카테고리 추가
data_tot['카테고리'] = data_tot['품목 이름'].apply(assign_category)

# 카테고리별로 데이터 분리
data_agriculture = data_tot[data_tot['카테고리'] == '농산물']
data_livestock = data_tot[data_tot['카테고리'] == '축산물']
data_seafood = data_tot[data_tot['카테고리'] == '생선/해산물']
data_sauce = data_tot[data_tot['카테고리'] == '젓갈/장/소스']
data_instant = data_tot[data_tot['카테고리'] == '가공식품']
data_beverage = data_tot[data_tot['카테고리'] == '음료']
data_dairy = data_tot[data_tot['카테고리'] == '유제품']
data_miscellaneous = data_tot[data_tot['카테고리'] == '기타']

# 카테고리별 데이터 확인
categories_data = {
    "농산물": data_agriculture,
    "축산물": data_livestock,
    "생선/해산물": data_seafood,
    "젓갈/장/소스": data_sauce,
    "가공식품": data_instant,
    "음료": data_beverage,
    "유제품": data_dairy,
    "기타": data_miscellaneous,
}

# 그룹화 결과 출력
print("\n\n------------------- 카테고리화 ---------------------")
print("==== 카테고리와 해당 품목 이름 출력 ====")
for category, df in categories_data.items():
    print(f"== {category} ==")
    print(df.groupby(['카테고리'])['품목 이름'].count())
    print("\n")


print("카테고리 null 개수: ")
print(data_tot['카테고리'].isnull().sum())


# 각 카테고리별 개수 확인
categorized_count = sum(
    len(df) for df in [
        data_agriculture, data_livestock, data_seafood,
        data_sauce, data_instant,
        data_beverage, data_dairy
    ]
)

# 누락된 데이터 개수 확인
missing_count = len(data_tot) - categorized_count
print(f"총 데이터 개수: {len(data_tot)}")
print(f"분류된 데이터 개수 합계: {categorized_count}")
print(f"누락된 데이터 개수: {missing_count}")


# 데이터프레임 정의
dataframes = {
    "data_agriculture": data_agriculture,
    "data_livestock": data_livestock,
    "data_seafood": data_seafood,
    "data_sauce": data_sauce,
    "data_instant": data_instant,
    "data_beverage": data_beverage,
    "data_dairy": data_dairy
}

# 각 데이터프레임에서 '품목 이름'의 고유 값 확인
for name, df in dataframes.items():
    print(f"=== {name} ===")
    print(df['품목 이름'].unique())
    print("\n")


# ----------- 가격 이상치 제거 ----------------
print("\n\n-------------- 가격 이상치 제거 -------------")
def remove_outliers(group, col):
    # IQR 방식으로 이상치 제거
    q1 = group[col].quantile(0.25)
    q3 = group[col].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return group[(group[col] >= lower_bound) & (group[col] <= upper_bound)]

# 카테고리별로 이상치 제거
filtered_dataframes = {}
for category, df in dataframes.items():
    filtered_dataframes[category] = (
        df.groupby('품목 이름', group_keys=False)  # group_keys=False로 그룹 키 제거
        .apply(remove_outliers, col='가격(원)')
    )

for category, df in dataframes.items():
    filtered_df = filtered_dataframes.get(category, None)
    if filtered_df is not None:
        print(f"=== {category} ===")
        print(f"Original Shape: {df.shape}, Filtered Shape: {filtered_df.shape}")
        print(f"Original Price Stats:\n{df['가격(원)'].describe()}")
        print(f"Filtered Price Stats:\n{filtered_df['가격(원)'].describe()}\n")


# '가격(원)'이 0인 행 제거
data_tot = data_tot[data_tot['가격(원)'] != 0]

# 결과 확인
print("==== 0원 데이터 제거 완료 ====")
print(f"남은 데이터 개수: {data_tot.shape[0]}")
print(f"0원 데이터 개수 확인: {(data_tot['가격(원)'] == 0).sum()}")

print("\n==== 품목별 데이터 개수와 이름 ====")
for category, df in filtered_dataframes.items():
    print(f"{category} 품목별 데이터 개수:")
    print(df['품목 이름'].value_counts())


print("\n\n---------- 데이터 저장 ------------")
import os

# 저장 디렉토리 설정
save_dir = os.path.join(os.getcwd(), "data_categorized")

# 각 데이터프레임을 CSV로 저장
for name, df in dataframes.items():
    save_path = os.path.join(save_dir, f"{name}.csv")
    df.to_csv(save_path, index=False, encoding="utf-8-sig")  # UTF-8 인코딩으로 저장
    print(f"{name} 데이터 저장 완료: {save_path}")