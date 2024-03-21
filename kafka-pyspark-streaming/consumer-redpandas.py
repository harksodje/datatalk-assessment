# !/usr/bin/env python3
from kafka import KafkaConsumer
import sys
from typing import Dict
# import time
import json
BOOSTRAP_SERVER = 'localhost:9092'

props = dict()
props.update(bootstrap_servers=BOOSTRAP_SERVER)
props.update(value_deserializer=lambda x: json.loads(x))

class RideConsumer():
    
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)
        
    def __repr__(self) -> str:
        return f'RideConsumer({self.__class__.__name__}) designed by Adis'
    
    @classmethod
    def boostrap_server(cls, props: Dict):
        return cls(props).consumer.bootstrap_connected()
    
    def consume(self, topic: str):
        self.consumer.subscribe(topic)
        for message in self.consumer:
            print(f"Consumed record for <key: {message.key}, value: {message.value}>")
            sys.stdout.flush()

# entry point
if __name__ == "__main__":
    consumer = RideConsumer(props=props)
    print(consumer)
    print(consumer.boostrap_server(props))
    consumer.consume('rides')