"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

from version import get_version

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# Init the faust app that will read in the stream from kafka connect and write out to a new topic
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Topic from which to read
topic = app.topic(f"arm.jdbc.v{get_version()}.stations", value_type=Station)
# Topic to which to write
out_topic = app.topic(f"arm.faust.v{get_version()}.stations.transformed", value_type=TransformedStation, partitions=1 )

table = app.Table(
    f"arm.faust.v{get_version()}.stations.table",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


# Using Faust, transforming input `Station` records into `TransformedStation` records.
# Example : If the `Station` record has the field `red` set to true, then set the `line` of the `TransformedStation` record to the string `"red"`

@app.agent(topic)
async def transform_station(station_events):
    async for station in station_events:
        if station.red:
            new_line = "red"
        elif station.blue:
            new_line = "blue"
        elif station.green:
            new_line = "green"
        else:
            new_line = "null"
        logger.info(f"Faust transforming into {new_line}")
        new_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=new_line
        )
        await out_topic.send(value=new_station)


if __name__ == "__main__":
    app.main()
