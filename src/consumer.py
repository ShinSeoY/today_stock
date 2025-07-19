import asyncio
from playwright.async_api import async_playwright
from collections import deque
import time
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from json import loads
import aiohttp


load_dotenv() 

BUFFER = deque()
MAX_BUFFER_SIZE = 10
MAX_WAIT_TIME = 60  # seconds
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_BROKERS = [KAFKA_HOST + ':9091', KAFKA_HOST + ':9092', KAFKA_HOST + ':9093']
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
API_SERVER_HOST = os.getenv("API_SERVER_HOST")

async def sendMail(payload):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{API_SERVER_HOST}/api/send-email", json=payload) as resp:
                if resp.status == 200:
                    print(f"✅ API 전송 성공")
                else:
                    print(resp)
                    raise Exception("❌ NOT 200 STATUS")
    except Exception as e:
        print(f"❌ API 전송 실패 : {e}")

async def processMessage(browser, code, email):
    try:
        page = await browser.new_page()
        await page.goto(f"https://m.stock.naver.com/domestic/stock/{code}/total")
        content = await page.inner_text('strong[class^="GraphMain_price"]')
        await page.close()

        payload = {
            "code": code,
            "email": email,
            "price": content
        }
        await sendMail(payload)

    except Exception as e:
        print(f"[❌ ERROR] {code} 처리 중 에러 발생: {e}")

def extract_raw_json(payload):
    try:
        raw_msg = json.dumps(payload)
        msg = json.loads(raw_msg)
        code = msg["code"]
        email = msg["email"]
        return (code, email)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"[❌ JSON ERROR] 메시지 파싱 실패: {e}")

async def start_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='produce',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        last_flush_time = time.time()

        try:
            while True:
                for message in consumer:
                    code, email = extract_raw_json(message.value)
                    if not code or not email:
                        continue

                    BUFFER.append((code.strip(), email.strip()))
                    print(f"[📥 수신] {code}, 현재 버퍼: {len(BUFFER)}")

                    if len(BUFFER) >= MAX_BUFFER_SIZE or (time.time() - last_flush_time > MAX_WAIT_TIME):
                        print(f"버퍼 처리 시작")
                        jobs = list(BUFFER)
                        BUFFER.clear()
                        last_flush_time = time.time()

                        tasks = [processMessage(browser, u, e) for u, e in jobs]
                        await asyncio.gather(*tasks)

                await asyncio.sleep(1)

        finally:
            await browser.close()
            consumer.close()