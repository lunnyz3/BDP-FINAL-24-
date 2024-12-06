# -*- coding: utf-8 -*-

import os
import pandas as pd
import chardet

# 현재 작업 디렉토리 기준으로 'data' 폴더 설정
base_dir = os.getcwd()  
data_dir = os.path.join(base_dir, "data")

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

file_paths = [os.path.join(data_dir, file_name) for file_name in file_names]

# 데이터를 담을 리스트
dataframes = []
for file_path in file_paths:
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())  # 파일의 인코딩 감지
        encoding = result["encoding"]
    try:
        # 시도 1: ';' 구분자 사용
        df = pd.read_csv(file_path, encoding=encoding, delimiter=";", skiprows=0)
    except:
        # 시도 2: ',' 구분자 사용
        df = pd.read_csv(file_path, encoding=encoding, delimiter=",", skiprows=0)

    dataframes.append(df)
# 데이터프레임 병합
data = pd.concat(dataframes, ignore_index=True)

# 문자열 데이터 전처리
for col in data.columns:
    if data[col].dtype == "object":  # 문자열 데이터만 처리
        data[col] = data[col].apply(lambda x: x.split('";')[0] if isinstance(x, str) and '";' in x else x)

# NaN 처리
data.replace(["nan", "NaN", ""], pd.NA, inplace=True)

# 불필요한 열 제거
for col in data.columns:
    if "일련번호," in col:  # 불필요한 열 조건
        data.drop(columns=[col], inplace=True)

# 년도-월 형식 확인
unique_year_months = data['년도-월'].unique()
print("고유한 년도-월 값:", unique_year_months)

import requests
import pandas as pd
from xml.etree import ElementTree

# API 정보
base_url = "http://openapi.seoul.go.kr:8088/5050624d5a636f6431303573446e6d4b/xml/ListNecessariesPricesService"
total_count = 97391  # 전체 데이터 개수
page_size = 1000  # 최대 1000개 호출 가능

all_data = []

# 페이지별로 데이터를 호출
for start in range(1, total_count + 1, page_size):
    end = min(start + page_size - 1, total_count)
    url = f"{base_url}/{start}/{end}/"
    print(f"현재 호출 중: {start}~{end}")

    response = requests.get(url)

    if response.status_code == 200:
        api_result = response.text

        try:
            root = ElementTree.fromstring(api_result)  # XML 파싱

            for row in root.findall("row"):
                record = {child.tag: (child.text if child.text else pd.NA) for child in row}  # None을 NaN으로 처리

                # 날짜 필터링: 2024년 이후 데이터만 추가
                p_date = record.get("P_DATE", "")  # "P_DATE" 필드 사용
                if p_date.startswith("2024"):
                    all_data.append(record)

        except ElementTree.ParseError as e:
            print(f"XML 파싱 오류 발생: {start}~{end}, 오류 내용: {e}")
            continue

    else:
        print(f"API 호출 실패: {start}~{end}, 상태 코드 {response.status_code}")
        continue

# Pandas DataFrame 생성
data_2024 = pd.DataFrame(all_data)

# 저장 폴더 설정
save_dir = os.path.join(os.getcwd(), "data_concatenated")
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# 2024년 데이터 csv로 저장
data_2024_path = os.path.join(save_dir, "data_2024.csv")
data_2024.to_csv(data_2024_path, index=False, encoding="utf-8-sig")
print(f"2024년 데이터 저장 완료: {data_2024_path}")

# csv 파일(13~23년도)과 api로 가져온 데이터(2024년도) 합치기
column_mapping = {
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

# 컬럼 이름 변경 및 데이터 타입 통일
data_2024.rename(columns=column_mapping, inplace=True)
data_2024_dtypes = data_2024.dtypes.to_dict()
for col, dtype in data_2024_dtypes.items():
    if col in data.columns:
        try:
            data[col] = data[col].astype(dtype)
        except ValueError:
            data[col] = pd.to_numeric(data[col], errors="coerce")

# 데이터 병합
data_tot = pd.concat([data_2024, data], ignore_index=True)

# 최종 병합 데이터 csv로 저장
total_file_path = os.path.join(save_dir, "data_total.csv")
data_tot.to_csv(total_file_path, index=False, encoding="utf-8-sig")
print(f"최종 병합 데이터 저장 완료: {total_file_path}")

# 결과 출력
print(data_tot.head())

# 년도-월 형식 확인
unique_year_months_tot = data_tot['년도-월'].unique()
print("고유한 년도-월 값:", unique_year_months_tot)