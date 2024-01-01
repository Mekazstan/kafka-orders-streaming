import json
import time
from config import config
from confluent_kafka import Consumer, KafkaException


# Topic Name
ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Function to update config for Consumer requests
def set_consumer_configs():
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

# Creating a callback function for partition assignment
def on_delivery(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
        
        
if __name__ == '__main__':
    set_consumer_configs()
    
    # Creating Consumer
    consumer = Consumer(config)
    consumer.subscribe([ORDER_KAFKA_TOPIC], on_assign=on_delivery)
    
    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:  
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                partition = event.partition()
                print(f'Received: {val} from partition {partition}    ')
                # consumer.commit(event)
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
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