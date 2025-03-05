import json
import random
from faker import Faker
import time
from confluent_kafka import Producer
import os
from dotenv import load_dotenv


def generateTransaction():
    fake = Faker()
    transaction = {
        "transaction_id": fake.uuid4(),
        "timestamp": int(time.time()),
        "user_id": random.randint(10000, 99999),
        "amount": round(random.uniform(5, 5000), 2),
        "transaction_type": random.choice(["purchase", "transfer", "withdrawal"]),
        "location": fake.city(),
        "merchant": fake.company(),
        "card_number": fake.credit_card_number()
    }

    return transaction


def delivery_report(err, msg):
    if err is not None:
        print(f"message is not delivered : {err}")
    else:
        print(f"message delivered to {msg.topic()}")


if __name__ == "__main__":
    load_dotenv("./.env")

    conf = {
        'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv("sasl.username"),
        'sasl.password': os.getenv("sasl.password")}

    producer = Producer(conf)

    while True:
        transaction = generateTransaction()
        producer.produce("fraud_detection", value=json.dumps(transaction), key=str(
            transaction['user_id']), callback=delivery_report)
        producer.flush()
        time.sleep(1)
