from config import config
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


# Topic Name
ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


##### Consumer Configurations #####
schema_registry_client = SchemaRegistryClient(config["schema_registry"])
avro_schema = schema_registry_client.get_latest_version(f"{ORDER_KAFKA_TOPIC}-value")
value_schema = avro_schema.schema.schema_str
avro_deserializer = AvroDeserializer(schema_registry_client, value_schema)


kafka_config = config["kafka"]
consumer_config = {
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': "orders_consumer_group",
    'auto.offset.reset': "latest"
}
consumer_config.update(kafka_config)

# Configuring Schema Registry
confirmed_schema = schema_registry_client.get_latest_version(f"{ORDER_CONFIRMED_KAFKA_TOPIC}-value")
        
# Producer Configuration
producer_config = {
    "key.serializer": StringSerializer(),
    "value.serializer": AvroSerializer(
        schema_registry_client,
        confirmed_schema.schema.schema_str
    )
}
producer_config.update(kafka_config)


def on_delivery(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Instantiating the Consumer
consumer = DeserializingConsumer(consumer_config)  

# Subscribe to the topic 'order_details'
consumer.subscribe([ORDER_KAFKA_TOPIC])  

# Creating the Producer
producer = SerializingProducer(producer_config)
             
try:
    print("Listening for Transactions..")
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        # Data Extraction
        consumed_message = msg.value()
        customer_id = consumed_message["CUSTOMER_ID"]
        customer_name = consumed_message["USERNAME"]
        email = consumed_message["EMAIL"]
        phone_number = consumed_message["PHONE_NUMBER"]
        address = consumed_message["ADDRESS"]
        total_cost = consumed_message["TOTAL_COST"]
        items_purchased = consumed_message["ITEMS"]
        order_date = consumed_message["ORDER_DATE"]
        payment_method = consumed_message["PAYMENT_METHOD"]
        age = consumed_message["AGE"]
        gender = consumed_message["GENDER"]
        
        
        data = {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "phone_number": phone_number,
                "address": address,
                "total_cost": total_cost,
                "items_purchased": items_purchased,
                "order_date": order_date,
                "payment_method": payment_method,
                "age": age,
                "gender": gender
            }
        
        customer_id = str(data["customer_id"])
        
        producer.produce(
            topic=ORDER_CONFIRMED_KAFKA_TOPIC,
            key=customer_id,
            value={
                "CUSTOMER_ID": data["customer_id"],
                "USERNAME": data["customer_name"],
                "EMAIL": data["email"],
                "PHONE_NUMBER": data["phone_number"],
                "ADDRESS": data["address"],
                "TOTAL_COST": data["total_cost"],
                "ITEMS": data["items_purchased"],
                "ORDER_DATE": data["order_date"],
                "PAYMENT_METHOD": data["payment_method"],
                "AGE": data["age"],
                "GENDER": data["gender"],
            },
            on_delivery=on_delivery)
        
        print(f"Successful Transaction made by: {data['customer_name']}...")

    producer.flush()

except KeyboardInterrupt:
    print("User Interrupt!!")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
