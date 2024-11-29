import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

# Selenium WebDriver 설정
driver = webdriver.Chrome()  # ChromeDriver가 설치된 경로가 환경 변수에 있어야 함

# 카테고리 URL과 이름 리스트
category_urls = [
    {"name": "채소", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213167"},
    {"name": "과일", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213114"},
    {"name": "쌀/잡곡/견과", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000215152"},
    {"name": "정육/계란류", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000215194"},
    {"name": "수산물/건해산", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213469"},
    {"name": "우유/유제품", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213534"},
    {"name": "밀키트/간편식", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213247"},
    {"name": "김치/반찬/델리", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213299"},
    {"name": "생수/음료/주류", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213424"},
    {"name": "커피/원두/차", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000215245"},
    {"name": "면류/통조림", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213319"},
    {"name": "양념/오일", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000215286"},
    {"name": "과자/간식", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213362"},
    {"name": "베이커리/잼", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213412"},
    {"name": "건강식품", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000213046"},
    {"name": "친환경/유기농", "url": "https://emart.ssg.com/disp/category.ssg?dispCtgId=6000228036"}
]

# 크롤링 데이터 저장용 리스트
all_products = []

# 각 카테고리별 크롤링
for category in category_urls:
    category_name = category["name"]
    category_url = category["url"]
    print(f"카테고리 크롤링 시작: {category_name} ({category_url})")
    page = 1  # 첫 번째 페이지부터 시작

    while True:
        # 페이지 URL 구성
        page_url = f"{category_url}&page={page}"
        driver.get(page_url)

        # 페이지 로딩 대기
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'emitem_info'))
            )
        except:
            print(f"페이지 로딩 실패: {page_url}")
            break

        # 페이지 소스 가져오기
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # 제품 정보 추출
        products = soup.find_all('a', class_='emitem_info')
        if not products:
            print(f"페이지 {page}에 더 이상 제품이 없습니다.")
            break

        print(f"페이지 {page}: {len(products)}개의 제품을 크롤링 중...")

        for product in products:
            # 제품 이름
            product_name_tag = product.find('span', class_='mnemitem_goods_tit')
            product_name = product_name_tag.text.strip() if product_name_tag else "N/A"

            # 제품 가격
            price_tag = product.find('em', class_='ssg_price')
            price = price_tag.text.strip() if price_tag else "N/A"

            # 데이터 저장
            all_products.append({
                "카테고리": category_name,
                "페이지": page,
                "제품명": product_name,
                "가격": price
            })

        # 다음 페이지로 이동
        page += 1
        time.sleep(2)  # 서버 부하 방지를 위해 대기 시간 추가

# 크롤링 완료
driver.quit()

# pandas DataFrame으로 변환
df = pd.DataFrame(all_products)

# 카테고리별로 CSV 저장
categories = df['카테고리'].unique()
for category in categories:
    category_df = df[df['카테고리'] == category]
    category_filename = f"{category}.csv"  # 카테고리 이름으로 파일명 생성
    category_df.to_csv(category_filename, index=False, encoding='utf-8-sig')

print("\nCSV 파일 저장 완료!")
