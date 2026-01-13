from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import csv

# Selenium WebDriver 설정
driver = webdriver.Chrome()  # ChromeDriver가 환경 변수에 등록되어 있어야 함

# 크롤링할 URL 리스트
urls = [
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029636",
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029667",
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029713",
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029732",
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029757",
    "https://shopping.naver.com/market/emarteveryday/category?menu=20029785"

]

# 크롤링 데이터 저장 리스트 및 세트
data = []
collected_product_ids = set()

# 무한 스크롤 및 데이터 수집 함수 정의
def scroll_and_collect_data(url):
    """특정 URL에서 스크롤을 진행하며 데이터를 수집"""
    driver.get(url)
    print(f"URL 접속: {url}")
    time.sleep(3)  # 페이지 로드 대기
    
    previous_height = driver.execute_script("return document.body.scrollHeight")
    index = 0  # 상품 번호 카운트
    while True:
        # 현재 화면에 로드된 상품 목록 가져오기
        products = driver.find_elements(By.CSS_SELECTOR, "li._3m7zfsGIZR")

        for product in products:
            try:
                # 상품 ID 추출 (상품 URL에서 추출)
                product_href = product.find_element(By.CSS_SELECTOR, 'a._3OaphyWXEP.linkAnchor').get_attribute('href')
                product_id = product_href.split('/')[-1].split('?')[0]

                if product_id not in collected_product_ids:
                    # 제목
                    title = product.find_element(By.CLASS_NAME, "_1J1f1i2vk0").text
                    # 가격
                    price = product.find_element(By.CLASS_NAME, "hDcL3_EQRC").text.replace("원", "").replace(",", "")
                    # 기본 단위당 가격
                    unit_price = product.find_element(By.CLASS_NAME, "_1RLV-iAg2_").text

                    # 데이터 추가
                    data.append([title, price, unit_price])  # URL도 저장
                    collected_product_ids.add(product_id)
                    index += 1
                    print(f"[{index}] 크롤링 성공: 제목={title}, 가격={price}원, 단위당 가격={unit_price}")
            except Exception as e:
                print(f"크롤링 실패: {e}")

        # 스크롤 다운
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)  # 스크롤 후 데이터 로드 대기

        # 새로운 높이 가져오기
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == previous_height:  # 더 이상 새로운 데이터가 없을 경우 종료
            break
        previous_height = new_height

try:
    print("여러 URL 크롤링 시작...")
    for url in urls:
        scroll_and_collect_data(url)
    print("모든 URL 크롤링 완료.")
    
finally:
    # 브라우저 닫기
    driver.quit()

# CSV 파일로 저장
csv_filename = "products.csv"
with open(csv_filename, "w", newline="", encoding="utf-8-sig") as csvfile:
    csvwriter = csv.writer(csvfile)
    # 헤더 작성
    csvwriter.writerow(["Title", "Price", "Unit Price"])
    # 데이터 작성
    csvwriter.writerows(data)

print(f"크롤링 완료! 데이터가 '{csv_filename}' 파일로 저장되었습니다.")
