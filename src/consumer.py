import asyncio
from playwright.async_api import async_playwright
# from aiokafka import AIOKafkaConsumer
from collections import deque
import time
from datetime import datetime
import json


# 버퍼 및 파라미터 설정
BUFFER = deque()
MAX_BUFFER_SIZE = 10
MAX_WAIT_TIME = 60  # seconds
# KAFKA_TOPIC = "scraping-jobs"
# KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

async def scrape_and_send_email(browser, code, email):
    try:
        page = await browser.new_page()
        await page.goto(f"https://m.stock.naver.com/domestic/stock/{code}/total")
        content = await page.inner_text('strong[class^="GraphMain_price"]')
        await page.close()

        # 이메일 전송 로직 (가정)
        print(f"[✅ EMAIL] {email}에게 '{code}'의 가격 {content} 전송")
    except Exception as e:
        print(f"[❌ ERROR] {code} 처리 중 에러 발생: {e}")

def extract_raw_json(raw_msg):
    try:
        msg = json.loads(raw_msg)
        code = msg["code"]
        email = msg["email"]
        return (code, email)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"[❌ JSON ERROR] 메시지 파싱 실패: {e}")


async def start_consumer():
    print('----')
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        last_flush_time = time.time()

        while True:
            raw_msg = "{ \"code\": \"360750\", \"email\": \"user@example.com\" }"

            if raw_msg is None:
                print(f"[{datetime.now()}] ❗️None msg")
                await asyncio.sleep(1)
                continue

            try:
                result = extract_raw_json(raw_msg)
                if result is None:
                    await asyncio.sleep(1)
                    continue

                code, email = result
                BUFFER.append((code.strip(), email.strip()))
                print(f"[{datetime.now()}] 메세지 수신 : {code}, 현재 버퍼: {len(BUFFER)}")

                if len(BUFFER) >= MAX_BUFFER_SIZE or (time.time() - last_flush_time > MAX_WAIT_TIME):
                    print(f"버퍼 처리 시작")
                    jobs = list(BUFFER)
                    BUFFER.clear()
                    last_flush_time = time.time()

                    tasks = [scrape_and_send_email(browser, u, e) for u, e in jobs]
                    await asyncio.gather(*tasks)

            except Exception as e:
                print(f"[{datetime.now()}]❗️[ERROR] 메시지 처리 실패: {e}")

            await asyncio.sleep(1)

    await browser.close()