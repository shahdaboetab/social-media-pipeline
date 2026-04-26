# Real-Time Social Media Data Pipeline

A real-time data pipeline built with Apache Kafka and Apache Spark Streaming
that ingests news articles from NewsAPI, filters them by keywords, and 
processes them in real time.

## Architecture
NewsAPI → Kafka Producer → Kafka Topic → Spark Streaming → Console Output

## Technologies
- Apache Kafka 3.7.0
- Apache Spark 3.5.1 (PySpark)
- NewsAPI
- Python 3.11

## Setup

### Requirements
- Java 11 or 17
- Python 3.11
- Apache Kafka

### Install dependencies
pip install kafka-python pyspark==3.5.1 requests

### Configuration
In producer.py replace:
API_KEY = "your_newsapi_key_here"
with your free API key from https://newsapi.org

## How to Run

1. Start Zookeeper
2. Start Kafka
3. Run producer.py
4. Run spark_consumer.py

## Project Structure
- producer.py — fetches and filters news, publishes to Kafka
- spark_consumer.py — consumes from Kafka, runs word count aggregation
- test_filter.py — unit tests for the filtering logic
