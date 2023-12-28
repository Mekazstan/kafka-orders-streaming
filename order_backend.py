import json
import time
from faker import Faker
from kafka import KafkaProducer

# Topic Name
ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20000

# Creating the Producer 
producer = KafkaProducer(bootstrap_servers="localhost:29092")
fake = Faker()

print("""
    Simulating Data generation from the Frontend Applications
        - A Unique order will be created every 5 seconds.  
    """)

for i in range(1, ORDER_LIMIT):
    data = {
        "customer_id": i,
        "username": fake.name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "total_cost": i * 5,
        "items": [fake.word() for _ in range(fake.random_int(min=1, max=5))],  # Generates a random list of items
        "order_date": fake.date_time_this_decade(),
        "payment_method": fake.random_element(elements=("Credit Card", "PayPal", "Cash")),
        "age": fake.random_int(min=18, max=80),
        "gender": fake.random_element(elements=("Male", "Female", "Other")),
    }
    
    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"{i}: {data['username']} just made an order...")
    time.sleep(5)