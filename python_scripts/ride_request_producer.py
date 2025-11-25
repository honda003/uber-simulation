import time
import random
import json
from kafka import KafkaProducer
from config import SECRETE

# Simple fake locations (same as drivers)
BOOTSTRAP_SERVERS = [
    SECRETE
]

LOCATIONS = ["u4pruy", "u4prut", "u4pruv", "u4prux", "u4pruz"]

def generate_request_event():
    return {
        "requestId": f"req_{random.randint(1, 9999)}",
        "userId": f"user_{random.randint(1, 20)}",
        "pickup": random.choice(LOCATIONS),
        "destination": random.choice(LOCATIONS),
        "timestamp": int(time.time())
    }

if __name__ == "__main__":
    print("🚕 Starting Ride Request Producer...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:
        event = generate_request_event()
        producer.send("ride-requests", value=event)
        print(f"➡️  Sent ride request: {event}")
        time.sleep(0.01)
