import json
import random
import time
from kafka import KafkaConsumer, KafkaProducer
from config import SECRETE

BOOTSTRAP_SERVERS = [
    SECRETE
]

# Consumer: reads matches from Ride Matcher
consumer = KafkaConsumer(
    "ride-matches",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

# Producer: sends confirmations back to riders
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚦 Ride Confirmator running... waiting for matches")

while True:
    for msg in consumer.poll(timeout_ms=100).values():
        for record in msg:
            match = record.value

            # Simulate driver accepts and price
            confirmation = {
                "requestId": match["requestId"],
                "driverId": match["driverId"],
                "price": round(random.uniform(5, 50), 2),
                "status": "ACCEPTED",
                "car": "Sedan",  # or random if you want
                "timestamp": int(time.time())
            }

            # Send confirmation to ride-confirmations topic
            producer.send("ride-confirmations", value=confirmation)
            producer.flush()

            print(f"💸 Ride confirmed: {confirmation}")

    
