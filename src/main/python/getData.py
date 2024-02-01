import requests
import json
import time
from kafka import KafkaProducer
import pandas as pd

def fetch_weather(url):
    response = requests.get(url)
    data = response.json()
    send_to_kafka(data)

def send_to_kafka(data):    
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092', value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    producer.send('weatherInfo', data)
    producer.flush()

df = pd.read_excel('.xlsx')

dt = df[df['adcode'].apply(lambda x: str(x)[-2:] != '00')]

for index, row in dt.iterrows():
    adcode = row['adcode']
    fetch_weather(f"https://restapi.amap.com/v3/weather/weatherInfo?city={adcode}&key=99e6222586ecc657a62761ef14376a56")

