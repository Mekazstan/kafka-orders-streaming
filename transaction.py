# import json
# import time
# from config import config
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.schema_registry.avro import AvroSerializer
# from confluent_kafka import DeserializingConsumer
# from confluent_kafka import SerializingProducer


# # Topic Name
# ORDER_KAFKA_TOPIC = "order_details"
# ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# # Creating Consumer

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