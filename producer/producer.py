import json
import time
import random
import threading
import os
import argparse
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

TOPICS = {
    'temperature': 'sensor_temperature',
    'sound': 'sensor_sound',
    'people': 'sensor_people'
}

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

def generate_temperature_data(device_id, room):
    return {
        "device": device_id,
        "type": "temperature",
        "room": str(room),
        "value": f"{random.uniform(18.0, 26.0):.1f}"
    }

def generate_sound_data(device_id, room):
    return {
        "device": device_id,
        "type": "sound",
        "room": str(room),
        "value": f"{random.uniform(30.0, 90.0):.1f}"
    }

def generate_people_data(device_id, room):
    return {
        "device": device_id,
        "type": "people_count",
        "room": str(room),
        "value": str(random.randint(0, 15))
    }

def start_producing(sensor_type, rate_per_second, generate_fn):
    device_id = f"{sensor_type}_1"
    room = random.randint(1, 20)
    interval = 1.0 / rate_per_second

    while True:
        data = generate_fn(device_id, room)
        json_data = json.dumps(data)
        producer.produce(
            TOPICS[sensor_type],
            key=device_id,
            value=json_data,
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(interval)

def run_all(rate_per_second=1):
    threading.Thread(target=start_producing, args=('temperature', rate_per_second, generate_temperature_data), daemon=True).start()
    threading.Thread(target=start_producing, args=('sound', rate_per_second, generate_sound_data), daemon=True).start()
    threading.Thread(target=start_producing, args=('people', rate_per_second, generate_people_data), daemon=True).start()

    print(f"📡 Sending sensor data to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ({rate_per_second} msg/sec per sensor type)...")
    while True:
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor Kafka Producer")
    parser.add_argument("--rps", type=int, default=1, help="Messages per second per sensor type")
    args = parser.parse_args()

    run_all(rate_per_second=args.rps)
