"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
ZOOKEEPER_URL = "localhost:2181"



class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Taking from https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html
        # and 
        # http://kafka.apache.org/documentation.html#brokerconfigs
        #
        #
        self.broker_properties = {
            "broker.id": 1,
            "bootstrap.servers": BROKER_URL,
            "log.dirs":"/tmp/kafka-logs",
            "schema.registry.url" : SCHEMA_REGISTRY_URL,
            "zookeeper.connect" : ZOOKEEPER_URL,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Taken from https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroProducer
        self.producer = AvroProducer(config=self.get_subdict(["bootstrap.servers", "schema.registry.url"]), 
            default_key_schema=key_schema, 
            default_value_schema=value_schema
        )
        logger.info(f"Producer created with topic name {self.topic_name}")

    
    def get_subdict(self, keys):
        return {k:self.broker_properties[k] for k in keys}
        
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-adminclient
        adminclient = AdminClient(self.get_subdict(["bootstrap.servers"]))
        nt = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas)
        
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.admin.AdminClient
        futures = adminclient.create_topics([ nt ])

        # As in the Kafka basics first example
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as e:
                logger.error(f"failed to create topic {self.topic_name}: {e}") 
 


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        self.producer.flush()

