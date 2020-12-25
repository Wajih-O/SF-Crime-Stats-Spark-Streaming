from producer_server import ProducerServer


def run_kafka_server():
	# get the data file path
    input_file = "../data/police-department-calls-for-service.json.gz"

    producer = ProducerServer(
        input_file=input_file,
        topic="sf.police_call_for_service",
        bootstrap_servers="localhost:9092",
        client_id="call_for_service_producer"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
