import argparse
import os
import time

from confluent_kafka import Message, Producer

from src.producer.event_generator import generate_customer_journey
from src.producer.event_model import RetailEvent

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "luxury-retail-events"


def delivery_report(error: Exception | None, message: Message) -> None:
    if error is not None:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered event to partition={message.partition()} offset={message.offset()}"
    )


def publish_event(
    producer: Producer,
    topic: str,
    event: RetailEvent,
) -> None:
    producer.produce(
        topic=topic,
        key=event.session_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


def run_producer(journey_count: int, delay: float) -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "luxury-retail-python-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    event_count = 0

    for journey_number in range(1, journey_count + 1):
        events = generate_customer_journey()

        print(
            f"\nJourney {journey_number}: "
            f"session={events[0].session_id}, events={len(events)}"
        )

        for event in events:
            publish_event(producer, topic, event)
            event_count += 1
            time.sleep(delay)

    remaining_messages = producer.flush(timeout=10)

    if remaining_messages:
        raise RuntimeError(f"{remaining_messages} Kafka messages were not delivered")

    print(f"\nSuccessfully published {event_count} events.")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish fictional luxury retail journeys to Kafka."
    )
    parser.add_argument(
        "--journeys",
        type=int,
        default=5,
        help="Number of customer journeys to generate.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between events.",
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    if arguments.journeys <= 0:
        raise ValueError("--journeys must be greater than zero")

    if arguments.delay < 0:
        raise ValueError("--delay cannot be negative")

    run_producer(arguments.journeys, arguments.delay)


if __name__ == "__main__":
    main()
