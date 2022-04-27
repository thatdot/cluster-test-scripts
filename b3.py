from utils.prodenv import *
from string import ascii_letters
import sys
from random import Random

"""
B3: Standing Query Stress Test
    1 ingest query
    Unbounded number of standing queries, one added every {wait_between_sqs_sec} seconds
Run: b3.py b3

As with all test scripts, ensure prodenv.py and variables at the top of the script reflect your
current Quine cluster before starting the test.

To run, you will need a kafka topic named {kafka_topic} ("hosts-proto" by default) containing
Protobuf-encoded messages cooresponding to the schema in host.proto
An easy way to populate such a topic is by running:
    produce_messages.py {kafka_topic} proto

The number of partitions on the topic should match the number of partitions expected
by utils/prodenv.py, 32 by default.

Monitor ingest rate via Grafana. Ingest rate should decrease cooresponding to the number of
Standing Queries registered.
"""


kafka_topic = "hosts-proto"
kafka_reset = "earliest"
group_id = f"b3-{Random().randint(1, 10*1000)}"

host_ingest_query = (
    """WITH locIdFrom($props.customer_id, 'host', $props.customer_id, $props.entity_id) AS hId """ +
    """MATCH (n) WHERE id(n) = hId """ +
    """SET n = $props, n:host """
)
ingest_streams = {
    "hosts": {
        "name": "hosts",
        "topic": kafka_topic,
        "query": host_ingest_query,
        "type": "Host",
        "kafka_reset": kafka_reset,
        "format": "PROTO",
        "group_id": group_id
    },
}
wait_between_sqs_sec = 30

r = Random(datagen_seed)


def query(pattern: str):
    return {
        "match": f"MATCH (n) WHERE n.hostname =~ '^{pattern}.*' RETURN DISTINCT id(n)",
        "action": (f"""MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
                   f"""MATCH (m) WHERE id(m) = locIdFrom(n.customer_id, n.customer_id, '{pattern}') """ +
                   f"""CREATE (n)-[:{pattern}]->(m) """ +
                   f"""SET m.name = "{pattern} BAZ", m:bar""")
    }


def register_query_for_pattern(pattern: str) -> bool:
    return register_standing_queries({
        f"{pattern}": query(pattern)
    })


def run_b3():
    checkConfig()
    startIngests(ingest_streams)
    print("Waiting 1 minute to get a baseline 1-minute ingest rate")
    sleep(60)
    queryAccepted = True
    while queryAccepted:
        pattern = ''.join(r.choice(ascii_letters) for _ in range(12))
        queryAccepted = register_query_for_pattern(pattern)
        sleep(wait_between_sqs_sec)
    print("Server rejected the most recent Standing Query")

if __name__ == "__main__":
    if len(sys.argv) > 1 and (sys.argv[1] == "run" or sys.argv[1] == "b3"):
        run_b3()
