# Scalable Decoupled Online Store Backend with Kafka Streaming (Confluent Kafka)
### This is a simulation of microservies implementation.
![alt text](https://github.com/Mekazstan/kafka-orders-streaming/blob/main/Model-Diagram.png)

This project focuses on building a scalable and decoupled backend for an online store, leveraging Kafka streaming for efficient communication between different components. The architecture involves several backend services that handle different aspects of the order processing workflow.


### Overview

The system is designed to handle the ordering process in an online store. It consists of several components, each serving a specific purpose:

    Frontend: Initiates the order process by sending a payload to the Order Backend.

    Order Backend: Takes the payload from the frontend, formats it, and produces it to the Order_details Kafka topic. This topic encapsulates information related to individual orders.

    Transaction Backend: Subscribes to the Order_details topic, consuming real-time order information. It processes the data, performs necessary transformations and checks, and then produces the results to the Order_confirmed Kafka topic.

    Email Backend: Subscribes to the Order_confirmed topic, consuming order confirmation messages. It sends email notifications to users when their orders are processed and confirmed.

    Analytics Backend: Also subscribes to the Order_confirmed topic, consuming order confirmation messages. It aggregates the data and creates visualizations for analytics purposes.

#### Note: In a real-world scenario, each backend service could be implemented as a microservice, providing scalability, maintainability, and flexibility.


### Workflow

    Order Placement: The frontend triggers the order process by sending a payload to the Order Backend Endpoint.

    Order Processing: The Order Backend Endpoint formats the payload and produces it to the Order_details Kafka topic.

    Real-time Transaction Processing: The Transaction Backend subscribes to the Order_details topic, processes the order information in real time, and produces the results to the Order_confirmed Kafka topic.

    Notification and Analytics: The Email Backend and Analytics Backend consume messages from the Order_confirmed topic. The Email Backend sends email notifications, while the Analytics Backend creates aggregated visualizations for analysis.

### Getting Started

To run the project locally, follow these steps:

    Set Up Kafka: Install and configure Kafka on your local machine. Make sure to create the necessary topics (Order_details and Order_confirmed).

    Backend Services: Set up each backend service (Order Backend Endpoint, Transaction Backend, Email Backend, and Analytics Backend). Ensure that they are configured to connect to the Kafka broker.

    Run the Frontend: Simulate frontend interactions by triggering order placements.

    Monitor Kafka Topics: Use Kafka tools to monitor the Order_details and Order_confirmed topics and observe the real-time flow of data.
