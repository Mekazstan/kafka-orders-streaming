import json
from kafka import KafkaConsumer

# Topic Name
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Creating Consumer
consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

total_orders_count = 0
total_revenue = 0
print("Listening for Events...")
while True:
    for message in consumer:
        print("Updating Analytics..")
        consumed_message = json.loads(message.value.decode())
        
        total_cost = float(consumed_message["total_cost"])
        items = float(consumed_message["items"])
        order_date = consumed_message["order_date"]
        payment_method = consumed_message["payment_method"]
        age = consumed_message["age"]
        gender = consumed_message["gender"]
        
        total_orders_count += 1
        total_revenue += total_cost
        
        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")