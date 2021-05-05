"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check
from version import get_version

logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088/ksql"

#
# Complete the following KSQL statements.
# For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
#  For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON
turnstile_topic = f"arm.stations.v{get_version()}.turnstile"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT, 
    station_name VARCHAR(100), 
    line VARCHAR(10)
) WITH ( 
    kafka_topic='arm.stations.v2.turnstile',
    value_format='AVRO', 
    key='station_id'
);

CREATE TABLE turnstile_summary 
WITH ( value_format = 'JSON') AS 
    SELECT station_id, 
        COUNT(station_id) AS count 
    FROM turnstile 
    GROUP BY station_id;
"""



def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return
    print("executing ksql statement...")
    logger.info("executing ksql statement...")

    resp = requests.post(
            KSQL_URL,
            headers={"Content-Type": "application/vnd.ksql.v1+json",
                     "Accept": "application/vnd.ksql.v1+json"},
            data=json.dumps(
                {
                    "ksql": KSQL_STATEMENT,
                    "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                }
            ),
        )
    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e: 
        print(e)
        logger.info("Error in KSQL post operation")
    

if __name__ == "__main__":
    execute_statement()
