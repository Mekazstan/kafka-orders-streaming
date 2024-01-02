import time
from faker import Faker
from config import config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

# Topic Name
ORDER_KAFKA_TOPIC = "order_details"

# Order Limit
ORDER_LIMIT = 10000

# Configuring Schema Registry
schema_registry_client = SchemaRegistryClient(config["schema_registry"])
orders_schema = schema_registry_client.get_latest_version("order_details-value")

# Updatig the config with serializers
kafka_config = config["kafka"]
kafka_config.update({
    "key.serializer": StringSerializer(),
    "value.serializer": AvroSerializer(
        schema_registry_client,
        orders_schema.schema.schema_str
    )
})


def on_delivery(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# Creating the Producer
producer = SerializingProducer(kafka_config)


print("""
    Simulating Data generation from the Frontend Applications
        - A Unique order will be created every 5 seconds.  
    """)


# Order transactions Simulation
fake = Faker()
for i in range(200, ORDER_LIMIT):
    data = {
        "customer_id": i,
        "username": fake.name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "total_cost": i * 5,
        "items": [fake.word() for _ in range(fake.random_int(min=1, max=5))],
        "order_date": fake.date_time_this_decade().isoformat(),
        "payment_method": fake.random_element(elements=("Credit Card", "PayPal", "Cash")),
        "age": fake.random_int(min=18, max=80),
        "gender": fake.random_element(elements=("Male", "Female", "Other")),
    }
    customer_id = str(data["customer_id"])
        
    producer.produce(
        topic=ORDER_KAFKA_TOPIC,
        key=customer_id,
        value={
            "CUSTOMER_ID": data["customer_id"],
            "USERNAME": data["username"],
            "EMAIL": data["email"],
            "PHONE_NUMBER": data["phone_number"],
            "ADDRESS": data["address"],
            "TOTAL_COST": data["total_cost"],
            "ITEMS": data["items"],
            "ORDER_DATE": data["order_date"],
            "PAYMENT_METHOD": data["payment_method"],
            "AGE": data["age"],
            "GENDER": data["gender"],
        },
        on_delivery=on_delivery
    )
    print(f"{data['username']} just made an order for {data['items']}")
    time.sleep(5)
        
producer.flush()