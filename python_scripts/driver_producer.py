import json
import random
import time
from kafka import KafkaProducer
from config import SECRETE

# -----------------------------
# CONFIG
# -----------------------------
BOOTSTRAP_SERVERS = [
    SECRETE
]

DRIVER_TOPIC = "drivers-status"

GEOHASHES = ["u4pruy", "u4pruv", "u4prux", "u4pruz", "u4prut"]
CAR_TYPES = ["Sedan", "SUV", "Van", "Compact"]

STATUSES = ["AVAILABLE", "BUSY", "OFFLINE"]

# -----------------------------
# KAFKA PRODUCER
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# -----------------------------
# DATA GENERATION
# -----------------------------
def generate_driver_event(driver_id):
    return {
        "driverId": driver_id,
        "status": random.choice(STATUSES),
        "location": f"({random.randint(0,100)},{random.randint(0,100)})",
        "locationId": random.choice(GEOHASHES),
        "car": random.choice(CAR_TYPES),
        "timestamp": int(time.time())
    }

# -----------------------------
# MAIN LOGIC
# -----------------------------
def start_driver_producer():
    print("🚗 Starting Driver Producer...")

    driver_ids = [f"driver_{i}" for i in range(1, 1000)] 

    while True:
        driver_id = random.choice(driver_ids)
        event = generate_driver_event(driver_id)

        # send event to Kafka
        producer.send(
            DRIVER_TOPIC,
            key=event["locationId"],
            value=event
        )

        print(f"➡️  Sent driver event: {event}")

        time.sleep(0.01)  


if __name__ == "__main__":
    start_driver_producer()
