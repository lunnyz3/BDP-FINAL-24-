from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import csv

# Selenium WebDriver 설정
driver = webdriver.Chrome()  # ChromeDriver가 환경 변수에 등록되어 있어야 함

url = "https://shopping.naver.com/market/emarteveryday/category"
driver.get(url)

# 크롤링 데이터 저장 리스트
data = []

# 무한 스크롤 함수 정의
def scroll_until_no_new_data():
    """더 이상 새로운 데이터가 없을 때까지 스크롤"""
    previous_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)  # 스크롤 후 데이터 로드 대기
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == previous_height:  # 더 이상 새로운 데이터가 없을 경우 종료
            break
        previous_height = new_height

try:
    print("무한 스크롤 시작...")
    scroll_until_no_new_data()
    print("스크롤 완료.")

    # 상품 목록 가져오기
    print("상품 정보 크롤링 시작...")
    products = driver.find_elements(By.CSS_SELECTOR, "li._3m7zfsGIZR")

    for index, product in enumerate(products, start=1):
        try:
            # 제목
            title = product.find_element(By.CLASS_NAME, "_1J1f1i2vk0").text
            # 가격
            price = product.find_element(By.CLASS_NAME, "hDcL3_EQRC").text.replace("원", "").replace(",", "")
            # 기본 단위당 가격
            unit_price = product.find_element(By.CLASS_NAME, "_1RLV-iAg2_").text

            # 데이터 추가
            data.append([title, price, unit_price])
            print(f"[{index}] 크롤링 성공: 제목={title}, 가격={price}원, 단위당 가격={unit_price}")
        except Exception as e:
            print(f"[{index}] 크롤링 실패: {e}")

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
