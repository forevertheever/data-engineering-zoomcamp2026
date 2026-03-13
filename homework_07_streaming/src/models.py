import json
import dataclasses
import pandas as pd
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    tip_amount: float
    total_amount: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    passenger_count: int


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        # lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        # lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        passenger_count=int(row['passenger_count'])
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


# def ride_deserializer(data):
#     json_str = data.decode('utf-8')
#     ride_dict = json.loads(json_str)
#     return Ride(**ride_dict)

def ride_deserializer(message_bytes):
    ride_dict = json.loads(message_bytes.decode('utf-8'))
    return Ride(**ride_dict)