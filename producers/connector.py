"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


from models.version import get_version

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    data_config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "topic.prefix": f"arm.jdbc.v{get_version()}.",
        "mode": "incrementing",
        "incrementing.column.name": "stop_id",
        "table.whitelist": "stations",
        "batch.max.rows": "500",
        "connection.url": "jdbc:postgresql://localhost:5432/cta",
        "connection.user": "cta_admin",
        "connection.password": "chicago",
        "poll.interval.ms": "10000",   
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": data_config
        }),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logger.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logger.info("connector created successfully.")
    logger.info("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()
