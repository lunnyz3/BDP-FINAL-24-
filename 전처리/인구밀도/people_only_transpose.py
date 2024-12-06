import chardet

# 파일의 인코딩 확인
file_path = 'C:/Users/USER/Desktop/people.csv'

with open(file_path, 'rb') as file:
    raw_data = file.read(10000)  # 파일의 일부만 읽어서 인코딩 감지
    result = chardet.detect(raw_data)
    print(f"추정된 인코딩: {result['encoding']}")

# UTF-8로 변환
import pandas as pd

# 올바른 인코딩으로 파일 읽기
encoding = result['encoding']  # 위에서 감지된 인코딩 값 사용
df = pd.read_csv(file_path, encoding=encoding)

# UTF-8로 저장
utf8_file_path = 'C:/Users/USER/Desktop/people_utf8.csv'
df.to_csv(utf8_file_path, index=False, encoding='utf-8')
print(f"파일이 UTF-8로 저장되었습니다: {utf8_file_path}")
