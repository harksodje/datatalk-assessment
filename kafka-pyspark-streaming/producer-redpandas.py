# !/usr/bin/env python3
from kafka import KafkaProducer
import sys
from typing import Dict
import time
import json
import pandas as pd
BOOSTRAP_SERVER = 'localhost:9092'



props = dict()
props.update(bootstrap_servers=BOOSTRAP_SERVER)
# props.update(key_serializer=lambda x: x.encode('utf-8'))
props.update(value_serializer=lambda x: json.dumps(x).encode('utf-8'))

class RiderProducer():

    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        
    def __repr__(self) -> str:
        return f'RiderProducer({self.__class__.__name__}) designed by Adis'
    
    @classmethod
    def boostrap_server(cls, props: Dict):
        return cls(props).producer.bootstrap_connected()
    
    def publish(self, topic: str):
        for i in range(10):
            key = f'key-{i}'
            message = {
                'number': i,
            }
            self.producer.send(topic, value=message)
            sys.stdout.write(f"Producing record for <key: {key}, value: {message}>\n",)
            time.sleep(0.05)
        self.producer.flush()
            
    def ride_csv_to_publish(self, resource_path: str, topic: str):
        df = pd.read_csv(resource_path, )
        df_req_columns = [
            "lpep_pickup_datetime", 
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "tip_amount",
        ]
        df_req = df[df_req_columns]
        for row in df_req.itertuples(index=False):
            self.producer.send(topic, value=row._asdict())
            sys.stdout.write(f"Producing record for <key: {row[0]}, value: {row._asdict()}>\n",)
            
        self.producer.flush()
        
    
# entry point
if __name__ == "__main__":
    producer = RiderProducer(props=props)
    # print(producer)
    # print(producer.boostrap_server(props))
    # init_time = time.time()
    # producer.publish('test-topic')
    # final_time = time.time()
    # print(f"Time taken to publish 10 records is {final_time - init_time} seconds")
    
    print("--------------#CSV to Kafka#-----------------")
    init_green_time = time.time()
    producer.ride_csv_to_publish('green_tripdata_2019-10.csv', 'green-trips')
    final_time_green = time.time()
    print(f"Time taken to publish 10 records is {final_time_green - init_green_time} seconds")
    