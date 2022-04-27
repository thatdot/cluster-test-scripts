from multiprocessing.connection import wait
from data_checker import DataChecker
from utils.prodenv import *
from string import ascii_letters
import sys
from random import Random
from datagen import Datagen
from time import perf_counter

"""
B5: LIMITed query run against supernode
Run: b5.py b5

As with all test scripts, ensure prodenv.py and variables at the top of the script reflect your
current Quine cluster before starting the test.

To run, you will need a kafka topic named {kafka_topic} ("hosts-proto" by default) containing
Protobuf-encoded messages cooresponding to the schema in host.proto
An easy way to populate such a topic is by running:
    produce_messages.py {kafka_topic} proto

This script assumes the nodes with the label :Customer will eventually become supernodes, and times
queries against an arbitrary :Customer node. The query being timed is a simple "retrieve 10 adjacent
nodes". These queries should return consistently and in a timely fashion.
"""

kafka_topic = "hosts-proto"
kafka_reset = "earliest"
group_id = f"b5-{Random().randint(1, 10*1000)}"

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
verification_poll_interval_sec = 10

sq_customer_match = """MATCH (n:host) WHERE exists(n.customer_id) RETURN DISTINCT id(n) AS id"""
sq_customer_action = (
    """MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
    """MATCH (m) WHERE id(m) = locIdFrom(n.customer_id, 'customer', n.customer_id) """ +
    """CREATE (n)-[:customer]->(m) """ +
    """SET m.customer_id = n.customer_id, m:Customer"""
)

all_standing_queries = {
    "customers": {
        "match": sq_customer_match,
        "action": sq_customer_action,
    }
}


def run_b5():
    checkConfig()
    startIngests(ingest_streams)
    register_standing_queries(all_standing_queries)
    # choose a customer to poll -- customers are supernodes, so this is best done early in the stream, before the node becomes "super"
    known_customer_id: str = None
    while not known_customer_id:
        query = f"""MATCH (n:Customer) RETURN strId(n) LIMIT 1"""
        api_result = requests.post(f"{a_quine_host}/api/v1/query/cypher", json={
            "text": query,
            "parameters": {}
        }).json()
        assert(api_result["columns"] == ["strId(n)"])
        query_results = api_result["results"]
        if query_results:
            assert(len(query_results) == 1) # There is only 1 result because of the LIMIT 1
            known_customer_id = query_results[0][0] # First column of the first result
            print(f"Found a customer to monitor: (stringified) id is '{known_customer_id}'")
    while True:
        sleep(verification_poll_interval_sec)
        query = f"""MATCH (n)--(m) WHERE strId(n) = '{known_customer_id}' RETURN m LIMIT 10"""
        # Time the query
        query_start_time = perf_counter()
        api_result = requests.post(f"{a_quine_host}/api/v1/query/cypher", json={
            "text": query,
            "parameters": {}
        }).json()
        query_finish_time = perf_counter()
        assert(api_result["columns"] == ["m"])
        print(
            f"""Query returned {len(api_result["results"])} nodes in {(query_finish_time-query_start_time):,.4f} seconds""")


if __name__ == "__main__":
    if len(sys.argv) > 1 and (sys.argv[1] == "run" or sys.argv[1] == "b5"):
        run_b5()
