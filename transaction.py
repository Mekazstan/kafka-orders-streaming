import json
import time
from config import config
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


# Topic Name
ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

schema_registry_url = 'https://psrc-l622j.us-east-2.aws.confluent.cloud'
bootstrap_servers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'

schema_registry_client = SchemaRegistryClient(config["schema_registry"])

avro_schema = schema_registry_client.get_latest_version(f"{ORDER_KAFKA_TOPIC}-value")
value_schema = avro_schema.schema.schema_str

avro_deserializer = AvroDeserializer(value_schema, schema_registry_client)

# Updatig the config with deserializers
kafka_config = config["kafka"]
kafka_config.update({
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': "orders_consumer_group",
    'auto.offset.reset': "latest"
})

# Instantiating the Consumer
consumer = DeserializingConsumer(kafka_config)  

# Subscribe to the topic 'order_details'
consumer.subscribe([ORDER_KAFKA_TOPIC])  
             
try:
    print("Listening for Transactions..")
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        # Print the consumed message
        print(f"Consumed message: {msg.value()}")

except KeyboardInterrupt:
    print("User Interrupt!!")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()



# consumer = KafkaConsumer(
#     ORDER_KAFKA_TOPIC,
#     bootstrap_servers="localhost:29092"
# )

# # Creating the Producer 
# producer = KafkaProducer(bootstrap_servers="localhost:29092")

# print("Listening for Transactions...")

# started = True

# while started:
#     print("Just starting..")
#     for message in consumer:
#         try:
#             print("Ongoing Transaction..")
            
#             # Decoding the encoded message
#             consumed_message = json.loads(message.value.decode())
#             print(consumed_message)
            
#             customer_id = consumed_message["customer_id"]
#             email = consumed_message["email"]
#             total_cost = consumed_message["total_cost"]
#             items = consumed_message["items"]
#             order_date = consumed_message["order_date"]
#             payment_method = consumed_message["payment_method"]
#             age = consumed_message["age"]
#             gender = consumed_message["gender"]
            
#             data = {
#                 "customer_id": customer_id,
#                 "customer_email": email,
#                 "total_cost": total_cost,
#                 "items": items,
#                 "order_date": order_date,
#                 "payment_method": payment_method,
#                 "age": age,
#                 "gender": gender
#             }
            
#             print("Successful Transaction..")
#             producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
            
#         except Exception as e:
#             print(f"Error: {e}")