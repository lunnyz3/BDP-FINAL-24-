import pandas as pd

# 파일 경로 설정
input_file = "C:/Users/USER/Desktop/생필품 농수축산물 가격 정보(2022년).csv"
output_file = "생필품 농수축산물 가격 정보(2022년)-utf8.csv"

# 데이터의 올바른 컬럼 정의
columns = [
    "일련번호", "시장/마트 번호", "시장/마트 이름", "품목 번호", "품목 이름",
    "실판매규격", "가격(원)", "년도-월", "비고", "시장유형 구분(시장/마트) 코드",
    "시장유형 구분(시장/마트) 이름", "자치구 코드", "자치구 이름", "점검일자"
]

# 원본 파일 읽기 (잘못된 줄 무시 처리 추가)
original_df = pd.read_csv(
    input_file,
    sep=";",  # 세미콜론 구분자
    engine="python",
    encoding="cp949",
    quotechar='"',  # 따옴표 처리
    header=None,  # 헤더가 없음을 명시
    names=columns,  # 직접 정의한 컬럼 이름 사용
    on_bad_lines="skip"  # 잘못된 줄 무시
)

# 원본 파일의 행 개수 출력
print(f"원본 파일 행 개수: {len(original_df)}")

# 변환된 데이터 저장 (헤더 포함)
original_df.to_csv(output_file, index=False, encoding="utf-8", sep=",", header=True)

# 변환된 파일 다시 읽기
converted_df = pd.read_csv(output_file)

# 변환된 파일의 행 개수 출력
print(f"변환된 파일 행 개수: {len(converted_df)}")
