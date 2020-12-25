import gzip
import json
import time
import logging


from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BROKER_URL = "localhost:9092"

def create_topic(topic_name, validate_only=True):
    """Creates the topic with the given topic name"""
    if topic_name in KafkaClient(BROKER_URL).topic_partitions:
        logger.warning('topic already exists!')
        return
    response = KafkaAdminClient(bootstrap_servers=BROKER_URL).create_topics(
       new_topics=[NewTopic(name=topic_name, num_partitions=5, replication_factor=1)],
       validate_only=validate_only
    )
    logger.info(response)


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        create_topic(self.topic)

    def generate_data(self):
        # as the data file is not a newline-separted json strings but rather a json array
        with gzip.open(self.input_file, 'r') as f:
            messages_as_dicts = json.loads(f.read())

        for message_dict in messages_as_dicts:
            message = self.dict_to_binary(message_dict)
            if message is not None:
                self.send(self.topic, message)
            time.sleep(1)

    def dict_to_binary(self, json_dict):
        """ return the json dict to binary """
        try:
            return json.dumps(json_dict).encode('utf-8')
        except Exception as e:
            logger.error(f"could not load: {json_str}")
            return None