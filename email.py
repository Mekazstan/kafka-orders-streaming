import json
from kafka import KafkaConsumer

# Topic Name
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Creating Consumer
consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

emails_sent_to = set()
print("Listening for Events...")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        
        print(f"Sending confirmation email to {customer_email}..")
        
        emails_sent_to.add(customer_email)
        if len(emails_sent_to) > 1:
            print(f"{len(emails_sent_to)} emails sent ..")
        else:
            print(f"{len(emails_sent_to)} email sent ..")