from src.producer.event_generator import generate_customer_journey


def main() -> None:
    events = generate_customer_journey()

    for event in events:
        print(event.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
