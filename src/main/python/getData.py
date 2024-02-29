import requests
import json
import schedule
import time

def fetch_weather_data():
    url = "高德天气API的URL"
    response = requests.get(url)
    data = response.json()
    # 处理和格式化数据
    send_to_kafka(data)

def send_to_kafka(data):
    # 发送数据到Kafka
    pass

schedule.every().hour.do(fetch_weather_data)

while True:
    schedule.run_pending()
    time.sleep(1)
