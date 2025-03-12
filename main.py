from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer
import json
import asyncio

app = FastAPI()

# Create Kafka consumer
consumer = KafkaConsumer(
    'route_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

latest_data = {}

# Async function to read data from Kafka
async def kafka_consumer():
    global latest_data
    for message in consumer:
        latest_data = message.value
        print(f"Received data: {latest_data}")
        await asyncio.sleep(1)

@app.on_event("startup")
async def start_kafka_consumer():
    # Start the Kafka consumer in the background
    asyncio.create_task(kafka_consumer())

class RouteOptimizationRequest(BaseModel):
    start: str
    destination: str

@app.post("/route-optimization")
async def route_optimization(route: RouteOptimizationRequest):
    traffic_condition = latest_data.get("traffic_condition", "unknown")
    weather_condition = latest_data.get("weather_condition", "unknown")

    optimized_route = optimize_route(route.start, route.destination, traffic_condition, weather_condition)

    return {
        "start": route.start,
        "destination": route.destination,
        "traffic_condition": traffic_condition,
        "weather_condition": weather_condition,
        "optimized_route": optimized_route
    }

def optimize_route(start, destination, traffic_condition, weather_condition):
    if traffic_condition == "heavy" and weather_condition == "rainy":
        return f"{start} -> Alternative Route -> {destination}"
    else:
        return f"{start} -> Direct Route -> {destination}"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
