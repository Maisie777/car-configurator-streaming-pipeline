from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Change to MSK broker if needed
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample config values
models = ["Roadster S", "Urban X", "Coupe R", "Adventure GT"]
colors = ["Midnight Black", "Pearl White", "Crimson Red", "Ocean Blue"]
engines = ["Electric", "Hybrid", "Diesel", "Petrol"]
interiors = ["Leather Beige", "Carbon Black", "Alcantara Grey"]

def generate_event():
    return {
        "timestamp": int(time.time()),
        "session_id": f"sess_{random.randint(1000,9999)}",
        "user_id": f"user_{random.randint(1,50)}",
        "model": random.choice(models),
        "color": random.choice(colors),
        "engine": random.choice(engines),
        "interior": random.choice(interiors),
        "price": round(random.uniform(65000, 125000), 2),
        "step": random.choice(["selected_model", "selected_color", "selected_engine", "confirmed_build"])
    }

# Send events continuously
while True:
    event = generate_event()
    producer.send('configurator-events', event)
    print(f"Sent: {event}")
    time.sleep(1)
