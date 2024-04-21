from confluent_kafka import Producer
import requests
import json

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'weather_data'
OPENWEATHER_API_KEY = '81c744833e2031b8e82a0ae18a1cbeed'
CITY = 'London'  

def fetch_weather_data():
    url = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={OPENWEATHER_API_KEY}'
    response = requests.get(url)
    return response.json()

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_weather_data():
    p = Producer({'bootstrap.servers': KAFKA_BROKER})

    weather_data = fetch_weather_data()

    # Produce message to Kafka topic
    p.produce(KAFKA_TOPIC, json.dumps(weather_data).encode('utf-8'), callback=delivery_report)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    p.poll(1)

    # Wait until all messages have been delivered
    p.flush()

if __name__ == '__main__':
    produce_weather_data()
