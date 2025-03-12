from kafka import KafkaProducer
import json
import random
import time

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Generate random traffic and weather data
def generate_random_data():
    traffic_condition = random.choice(["heavy", "moderate", "light"])
    weather_condition = random.choice(["clear", "rainy", "snowy", "foggy"])
    return {
        "timestamp": time.time(),
        "traffic_condition": traffic_condition,
        "weather_condition": weather_condition
    }

# Send data to Kafka
while True:
    data = generate_random_data()
    producer.send('route_data', data)
    print(f"Sent data: {data}")
    time.sleep(5)  # Send data every 5 seconds
