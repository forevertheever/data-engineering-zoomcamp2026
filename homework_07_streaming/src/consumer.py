from kafka import KafkaConsumer
from pyarrow import json
from models import Ride, ride_deserializer
from datetime import datetime

server = 'localhost:9092'
# server = 'redpanda:29092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=5000
)
print(f"Listening to {topic_name}...")

total_messages = 0
long_trips = 0

for message in consumer:
    ride = message.value
    total_messages += 1

    if ride.trip_distance > 5:
        long_trips += 1

    # if total_messages % 100 == 0:
    #     print(f"Processed {total_messages} trips, trips >5 distance: {long_trips}")

print(f"Final count of trips with distance > 5: {long_trips}")

consumer.close()