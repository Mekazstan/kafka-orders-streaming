from config import config
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


# Topic Name
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


##### Consumer Configurations #####
schema_registry_client = SchemaRegistryClient(config["schema_registry"])
avro_schema = schema_registry_client.get_latest_version(f"{ORDER_CONFIRMED_KAFKA_TOPIC}-value")
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

# Instantiating the Consumer
consumer = DeserializingConsumer(consumer_config)  

# Subscribe to the topic 'order_details'
consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC])  

emails_sent_to = set()
             
try:
    print("Listening for Messages..")
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        # Data Extraction
        consumed_message = msg.value()
        email = consumed_message["EMAIL"] 
        emails_sent_to.add(email)
        
        if len(emails_sent_to) > 1:
            print(f"""
                  {len(emails_sent_to)} email sent
                  {email} was added to the list.
                  """)
        else:
            print(f"""
                  {len(emails_sent_to)} email sent
                  {email} was added to the list.
                  """)

except KeyboardInterrupt:
    print("User Interrupt!!")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
