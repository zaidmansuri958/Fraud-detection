from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json


def if_fradulent(transaction):
    fraud_score = transaction['amount']/5000
    return fraud_score > 0.8


if __name__ == "__main__":

    load_dotenv("./.env")

    conf = {
        'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv("sasl.username"),
        'sasl.password': os.getenv("sasl.password"),
        'group.id': os.getenv("KAFKA_GROUP_ID"),
        'auto.offset.reset': 'earliest'
    }

    clinet = MongoClient(os.getenv("MONGO_URI"))
    df = clinet[os.getenv("MONGO_DB")]
    collection = df[os.getenv("MONGO_COLLECTION")]
    consumer = Consumer(conf)
    consumer.subscribe([os.getenv("KAFKA_TOPIC")])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"kafka error not able to get message {msg.error()}")
                continue
            transaction = json.loads(msg.value().decode('utf-8'))
            print(f"Received message from kafka {transaction}")

            if if_fradulent(transaction):
                print("fraud detected : ", transaction)
                collection.insert_one(transaction)
    except Exception as e:
        print(e)

    finally:
        consumer.close()
