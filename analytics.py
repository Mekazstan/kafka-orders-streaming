import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
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

# Initialize Dash app
app = dash.Dash(__name__)

# Layout of the web app
app.layout = html.Div([
    dcc.Graph(id='orders-count-over-time'),
    dcc.Graph(id='revenue-over-time'),
    dcc.Graph(id='payment-method-distribution'),
    dcc.Graph(id='age-distribution'),
    dcc.Graph(id='gender-distribution'),
    dcc.Graph(id='avg-order-value-over-time'),
    dcc.Graph(id='popular-items'),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds (update every 5 seconds)
        n_intervals=0
    ),
])

# Callback to update charts dynamically
@app.callback(
    [Output('orders-count-over-time', 'figure'),
     Output('revenue-over-time', 'figure'),
     Output('payment-method-distribution', 'figure'),
     Output('age-distribution', 'figure'),
     Output('gender-distribution', 'figure'),
     Output('avg-order-value-over-time', 'figure'),
     Output('popular-items', 'figure')],
    [Input('interval-component', 'n_intervals')]
)

def update_charts(n_intervals):
    total_orders_count = 0
    total_revenue = 0
    order_dates = []
    orders_count_data = []
    revenue_data = []
    payment_method_distribution = {}
    age_distribution = {}
    gender_distribution = {}
    avg_order_value_data = []
    popular_items_data = {}

    # Poll for a message from the Kafka topic
    msg = consumer.poll(1.0)

    if msg is not None:
        consumed_message = msg.value()

        # Extracted Columns
        total_cost = float(consumed_message["TOTAL_COST"])
        items = consumed_message["ITEMS"]
        order_date = consumed_message["ORDER_DATE"]
        payment_method = consumed_message["PAYEMENT_METHOD"]
        age = consumed_message["AGE"]
        gender = consumed_message["GENDER"]

        total_orders_count += 1
        total_revenue += total_cost

        # Update data for each visualization
        order_dates.append(order_date)
        orders_count_data.append(total_orders_count)
        revenue_data.append(total_revenue)

        if payment_method in payment_method_distribution:
            payment_method_distribution[payment_method] += 1
        else:
            payment_method_distribution[payment_method] = 1

        if age in age_distribution:
            age_distribution[age] += 1
        else:
            age_distribution[age] = 1

        if gender in gender_distribution:
            gender_distribution[gender] += 1
        else:
            gender_distribution[gender] = 1

        avg_order_value_data.append(total_revenue / total_orders_count)

        for item in items:
            if item in popular_items_data:
                popular_items_data[item] += 1
            else:
                popular_items_data[item] = 1

    # Update the charts
    orders_count_fig = px.line(x=order_dates, y=orders_count_data, labels={'x': 'Order Date', 'y': 'Total Orders Count'},
                                title='Total Orders Count Over Time')
    revenue_fig = px.line(x=order_dates, y=revenue_data, labels={'x': 'Order Date', 'y': 'Total Revenue'},
                            title='Revenue Over Time')
    payment_method_fig = px.pie(values=list(payment_method_distribution.values()), names=list(payment_method_distribution.keys()),
                                title='Distribution of Payment Methods')
    age_fig = px.bar(x=list(age_distribution.keys()), y=list(age_distribution.values()), labels={'x': 'Age', 'y': 'Count'},
                        title='Age Distribution of Customers')
    gender_fig = px.pie(values=list(gender_distribution.values()), names=list(gender_distribution.keys()),
                        title='Gender Distribution of Customers')
    avg_order_value_fig = px.line(x=order_dates, y=avg_order_value_data,
                                    labels={'x': 'Order Date', 'y': 'Average Order Value'},
                                    title='Average Order Value Over Time')
    popular_items_fig = px.bar(x=list(popular_items_data.keys()), y=list(popular_items_data.values()),
                                labels={'x': 'Item', 'y': 'Count'},
                                title='Popular Items Ordered')

    return orders_count_fig, revenue_fig, payment_method_fig, age_fig, gender_fig, avg_order_value_fig, popular_items_fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)



