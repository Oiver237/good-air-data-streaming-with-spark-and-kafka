from kafka import KafkaConsumer
topics=['weather_data','air_quality_data']

for topic in topics:
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    for message in consumer:
        print(message.value.decode('utf-8'))
