import json
import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row, ride_serializer
import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

# producer = KafkaProducer(
#     bootstrap_servers=[server],
#     value_serializer=json_serializer
# )

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

# producer.bootstrap_connected()

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'
# url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.parquet'
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'tip_amount', 'total_amount', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count']
df = pd.read_parquet(url, columns=columns)
# print(df.head(10))
# df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
# df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)
df['passenger_count'] = df['passenger_count'].fillna(0)
df['passenger_count'] = df['passenger_count'].astype(int)


t0 = time.time()
topic_name = 'green-trips'

for _, row in df.iterrows():
    message = ride_from_row(row)
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    # time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')