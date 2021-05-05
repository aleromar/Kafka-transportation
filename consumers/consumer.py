
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
ZOOKEEPER_URL = "localhost:2181"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # Broker properties
        self.broker_properties = {
            "broker.id": 1,
            "bootstrap.servers": BROKER_URL,
            "log.dirs":"/tmp/kafka-logs",
            "zookeeper.connect" : ZOOKEEPER_URL,
            "group.id" : "my-consumer-group",
            'auto.offset.reset': 'earliest'
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(config=self.get_subdict(["bootstrap.servers", "schema.registry.url","group.id","auto.offset.reset"]))
            
        else:
            self.consumer = Consumer(config=self.get_subdict(["bootstrap.servers","group.id","auto.offset.reset"]))


        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe(
            [self.topic_name_pattern], on_assign=self.on_assign)
                                    
    def get_subdict(self, keys):
        return {k:self.broker_properties[k] for k in keys}
    
    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
            
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        message = self.consumer.poll(self.consume_timeout)
        if message is None:
            logger.info("No message received by consumer.")
            return 0
        elif message.error() is not None:
            logger.error(f"error from consumer {message.error()}")
            return 0
        else:
            self.message_handler(message)
            return 1


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info("Shutting down consumer.")
