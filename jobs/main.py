from confluent_kafka import Producer
import requests
import json
from config import configuration

KAFKA_BROKER = 'localhost:9092'
WEATHER_KAFKA_TOPIC = 'weather_data'
AIR_QUALITY_KAFKA_TOPIC = 'air_quality_data'
OPENWEATHER_API_KEY = configuration.get('OPENWEATHER_API_KEY')
WAQI_API_TOKEN = configuration.get('WAQI_API_TOKEN')
CITIES = ['Paris','Marseille', 'Lyon', 'Lille', 'Nantes', 'Bordeaux', 'Toulouse', 'Nice', 'Montpellier',' Strasbourg']  

def fetch_weather_data(city):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}'
    response = requests.get(url)
    return response.json()

def fetch_air_quality_data(city):
    url = f'https://api.waqi.info/feed/{city}/?token={WAQI_API_TOKEN}'
    response = requests.get(url)
    return response.json()

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_weather_data():
    p = Producer({'bootstrap.servers': KAFKA_BROKER})

    for city in CITIES:
        weather_data = fetch_weather_data(city)

        p.produce(WEATHER_KAFKA_TOPIC, json.dumps(weather_data).encode('utf-8'), callback=delivery_report)

        p.poll(1)

        air_quality_data = fetch_air_quality_data(city)

        p.produce(AIR_QUALITY_KAFKA_TOPIC, json.dumps(air_quality_data).encode('utf-8'), callback=delivery_report)

        p.poll(1)

    p.flush()

if __name__ == '__main__':
    produce_weather_data()
