import json
import time
import random
from faker import Faker
from confluent_kafka import Producer

# 1. Kafka client. Broker are in our localhost. Post 9092 (See docker-compose.yml)
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'fintech-transaction-generator'
}
producer = Producer(config)

# Start Faker, to create realistic data
fake = Faker()

# Callback function. Check if Kafka receive the message correctly
def delivery_report(err, msg):

    if err is not None:
        print( f"error sending message: {err}" )
    else:
        print(f"message sending to {msg.topic()} [Partition: {msg.partition()}]")

print("Generator of financial transactions")

try:

    while True:

        # 2. Create a fake transaction
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": f"USER_{fake.random_int(1000,9999)}",
            "amount": round(random.expovariate(1/100), 2),
            "merchant": fake.company(),
            "location": fake.city(),
            "timestamp": int(time.time())
        }

        # 3. Dictionary to JSON
        payload = json.dumps(transaction).encode('utf-8')

        # 4. Send messsage
        producer.produce(
            topic = 'financial_transactions',
            value = payload,
            callback = delivery_report
        )

        # Flush message to buffer
        producer.poll(0)

        # Wait 0.5 seconds
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Keyboard interruption")
finally:
    # A final flushing 
    producer.flush()