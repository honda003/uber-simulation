import json
from kafka import KafkaConsumer, KafkaProducer
from config import SECRETE

BOOTSTRAP_SERVERS = [
    SECRETE
]

# Consumer: reads drivers-status and ride-requests
driver_consumer = KafkaConsumer(
    "drivers-status",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

request_consumer = KafkaConsumer(
    "ride-requests",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

# Producer: publishes matches
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚦 Ride Matcher is running... waiting for events")

drivers_state = {}  # Keep drivers in memory

while True:
    # Read drivers
    for msg in driver_consumer.poll(timeout_ms=100).values():
        for record in msg:
            driver = record.value
            drivers_state[driver["driverId"]] = driver

            print(f"🚖 Driver update: {driver}")

    # Read ride requests
    for msg in request_consumer.poll(timeout_ms=100).values():
        for record in msg:
            request = record.value
            print(f"🆕 New ride request: {request}")

            # Simple matching: pick the first available driver
            available = [
                d for d in drivers_state.values()
                if d["status"] == "AVAILABLE"
            ]


            if not available:
                print("❌ No available drivers")
                continue

            driver = available[0]   # Very simple matching

            match_event = {
                "requestId": request["requestId"],
                "driverId": driver["driverId"],
                "pickup": request["pickup"],
                "timestamp": request["timestamp"]
            }

            producer.send("ride-matches", value=match_event)
            producer.flush()

            print(f"💚 MATCHED: {match_event}")
