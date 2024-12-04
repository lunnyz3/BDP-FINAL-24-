import pandas as pd

# CSV 파일 경로
file_path = 'C:/Users/USER/Desktop/people.csv'

# CSV 파일 읽기
df = pd.read_csv(file_path, header=[0, 1])  # 멀티 인덱스로 읽기

# 1. "계"에 대한 정보만 필터링
filtered_df = df[df[('항목', '항목')] == '계']

# 2. 데이터 전치와 날짜 정보 보존
transposed_df = filtered_df.set_index(('동별(1)', '동별(1)')).iloc[:, 2:]
transposed_df.columns = transposed_df.columns.get_level_values(0)  # 날짜 정보만 가져오기

# 데이터 전치
transposed_df = transposed_df.T

# 날짜를 행으로 변환
transposed_df.reset_index(inplace=True)
transposed_df.rename(columns={'index': '날짜'}, inplace=True)

# 최종 데이터프레임 확인
print(transposed_df)

# 결과를 CSV로 저장하려면 아래 코드 사용 (선택 사항)
transposed_df.to_csv('transformed_people.csv', encoding='utf-8-sig', index=False)
