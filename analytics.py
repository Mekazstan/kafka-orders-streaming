# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# from dash.dependencies import Input, Output
# import plotly.express as px
# import json
# from kafka import KafkaConsumer

# # Topic Name
# ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# # Creating Consumer
# consumer = KafkaConsumer(
#     ORDER_CONFIRMED_KAFKA_TOPIC,
#     bootstrap_servers="localhost:29092"
# )

# # Initialize Dash app
# app = dash.Dash(__name__)

# # Layout of the web app
# app.layout = html.Div([
#     dcc.Graph(id='orders-count-over-time'),
#     dcc.Graph(id='revenue-over-time'),
#     dcc.Graph(id='payment-method-distribution'),
#     dcc.Graph(id='age-distribution'),
#     dcc.Graph(id='gender-distribution'),
#     dcc.Graph(id='avg-order-value-over-time'),
#     dcc.Graph(id='popular-items'),
#     dcc.Interval(
#         id='interval-component',
#         interval=5 * 1000,  # in milliseconds (update every 5 seconds)
#         n_intervals=0
#     ),
# ])

# # Callback to update charts dynamically
# @app.callback(
#     [Output('orders-count-over-time', 'figure'),
#      Output('revenue-over-time', 'figure'),
#      Output('payment-method-distribution', 'figure'),
#      Output('age-distribution', 'figure'),
#      Output('gender-distribution', 'figure'),
#      Output('avg-order-value-over-time', 'figure'),
#      Output('popular-items', 'figure')],
#     [Input('interval-component', 'n_intervals')]
# )
# def update_charts(n_intervals):
#     total_orders_count = 0
#     total_revenue = 0
#     order_dates = []
#     orders_count_data = []
#     revenue_data = []
#     payment_method_distribution = {}
#     age_distribution = {}
#     gender_distribution = {}
#     avg_order_value_data = []
#     popular_items_data = {}

#     for message in consumer:
#         consumed_message = json.loads(message.value.decode())

#         # Extracted Columns
#         total_cost = float(consumed_message["total_cost"])
#         items = consumed_message["items"]
#         order_date = consumed_message["order_date"]
#         payment_method = consumed_message["payment_method"]
#         age = consumed_message["age"]
#         gender = consumed_message["gender"]

#         total_orders_count += 1
#         total_revenue += total_cost

#         # Update data for each visualization
#         order_dates.append(order_date)
#         orders_count_data.append(total_orders_count)
#         revenue_data.append(total_revenue)

#         if payment_method in payment_method_distribution:
#             payment_method_distribution[payment_method] += 1
#         else:
#             payment_method_distribution[payment_method] = 1

#         if age in age_distribution:
#             age_distribution[age] += 1
#         else:
#             age_distribution[age] = 1

#         if gender in gender_distribution:
#             gender_distribution[gender] += 1
#         else:
#             gender_distribution[gender] = 1

#         avg_order_value_data.append(total_revenue / total_orders_count)

#         for item in items:
#             if item in popular_items_data:
#                 popular_items_data[item] += 1
#             else:
#                 popular_items_data[item] = 1

#         # Update the charts
#         orders_count_fig = px.line(x=order_dates, y=orders_count_data, labels={'x': 'Order Date', 'y': 'Total Orders Count'},
#                                    title='Total Orders Count Over Time')
#         revenue_fig = px.line(x=order_dates, y=revenue_data, labels={'x': 'Order Date', 'y': 'Total Revenue'},
#                               title='Revenue Over Time')
#         payment_method_fig = px.pie(values=list(payment_method_distribution.values()), names=list(payment_method_distribution.keys()),
#                                     title='Distribution of Payment Methods')
#         age_fig = px.bar(x=list(age_distribution.keys()), y=list(age_distribution.values()), labels={'x': 'Age', 'y': 'Count'},
#                          title='Age Distribution of Customers')
#         gender_fig = px.pie(values=list(gender_distribution.values()), names=list(gender_distribution.keys()),
#                             title='Gender Distribution of Customers')
#         avg_order_value_fig = px.line(x=order_dates, y=avg_order_value_data,
#                                      labels={'x': 'Order Date', 'y': 'Average Order Value'},
#                                      title='Average Order Value Over Time')
#         popular_items_fig = px.bar(x=list(popular_items_data.keys()), y=list(popular_items_data.values()),
#                                    labels={'x': 'Item', 'y': 'Count'},
#                                    title='Popular Items Ordered')

#         return orders_count_fig, revenue_fig, payment_method_fig, age_fig, gender_fig, avg_order_value_fig, popular_items_fig

# # Run the app
# if __name__ == '__main__':
#     app.run_server(debug=True)
















# # import json
# # from kafka import KafkaConsumer

# # # Topic Name
# # ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# # # Creating Consumer
# # consumer = KafkaConsumer(
# #     ORDER_CONFIRMED_KAFKA_TOPIC,
# #     bootstrap_servers="localhost:29092"
# # )

# # total_orders_count = 0
# # total_revenue = 0
# # print("Listening for Events...")
# # while True:
# #     for message in consumer:
# #         print("Updating Analytics..")
# #         consumed_message = json.loads(message.value.decode())
        
# #         total_cost = float(consumed_message["total_cost"])
# #         items = float(consumed_message["items"])
# #         order_date = consumed_message["order_date"]
# #         payment_method = consumed_message["payment_method"]
# #         age = consumed_message["age"]
# #         gender = consumed_message["gender"]
        
# #         total_orders_count += 1
# #         total_revenue += total_cost
        
# #         print(f"Orders so far today: {total_orders_count}")
# #         print(f"Revenue so far today: {total_revenue}")