# Luxury Retail Streaming Platform

A real-time data engineering project that simulates customer activity for a fictional luxury e-commerce platform.

The platform generates product views, cart activity and purchase events, validates them using a Python data contract, and publishes them to Apache Kafka for stream processing.

## Project objectives

- Build an event-driven data pipeline with Apache Kafka
- Process streaming data using Spark Structured Streaming
- Handle duplicate and late-arriving events
- Implement Bronze, Silver and Gold data layers
- Calculate retail metrics such as conversion rate and revenue
- Apply automated testing and data-quality checks

## Current architecture

```text
Customer journey generator
        ↓
Pydantic event validation
        ↓
Python Kafka producer
        ↓
Kafka topic: luxury-retail-events
        ↓
Spark Structured Streaming (next stage)
```

## Implemented

- Fictional luxury product catalogue
- Logical customer-journey generation
- Validated retail event schema
- Kafka broker running in Docker
- Three-partition Kafka topic
- Idempotent Python Kafka producer
- Automated unit tests and Ruff checks

## Technology stack

- Python 3.12
- Apache Kafka
- Docker Compose
- Pydantic
- Confluent Kafka Python client
- pytest
- Ruff

## Run the tests

```bash
uv run python -m pytest -v
uv run ruff check .
```

## Start Kafka

```bash
docker compose -f docker/compose.yml up -d
```

## Publish sample journeys

```bash
uv run python -m src.producer.kafka_producer --journeys 5 --delay 0.5
```

## Project status

In development. The next stage adds Spark Structured Streaming as the Kafka consumer.