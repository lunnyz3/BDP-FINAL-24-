import pandas as pd

# CSV 파일 읽기

file_path = 'C:/Users/USER/Desktop/people.csv'
df = pd.read_csv(file_path, header=[0, 1])  # 멀티 인덱스로 읽기

# "계"에 대한 정보만 필터링
filtered_df = df[df[('항목', '항목')] == '계']

# 데이터 전치
transposed_df = filtered_df.set_index(('동별(1)', '동별(1)')).T

# 컬럼 이름 정리 (날짜와 나이대 유지)
transposed_df.reset_index(inplace=True)
transposed_df.rename(columns={'level_0': '날짜', 'level_1': '나이대'}, inplace=True)

# CSV로 저장
output_path = 'transposed_people.csv'
transposed_df.to_csv(output_path, encoding='utf-8-sig', index=False)

# 결과 확인
transposed_df.head()
