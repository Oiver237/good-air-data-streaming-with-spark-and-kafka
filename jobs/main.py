from confluent_kafka import Producer
import requests
import json
from config import configuration

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'weather_data'
OPENWEATHER_API_KEY = configuration.get('API_KEY')
CITIES = ['Paris','Marseille', 'Lyon', 'Lille', 'Nantes', 'Bordeaux', 'Toulouse', 'Nice', 'Montpellier',' Strasbourg']  

def fetch_weather_data(city):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}'
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

        p.produce(KAFKA_TOPIC, json.dumps(weather_data).encode('utf-8'), callback=delivery_report)

        # Wait for delivery report
        p.poll(1)
    
    # Flush messages to ensure delivery
    p.flush()

if __name__ == '__main__':
    produce_weather_data()
