from multiprocessing.connection import wait
from data_checker import DataChecker
from utils.prodenv import *
from string import ascii_letters
import sys
from random import Random
from datagen import Datagen
from time import perf_counter, time

"""
B6: Paginated queries against a supernode
Run: b6.py b6

As with all test scripts, ensure prodenv.py and variables at the top of the script reflect your
current Quine cluster before starting the test.

To run, you will need a kafka topic named {kafka_topic} ("hosts-proto" by default) containing
Protobuf-encoded messages cooresponding to the schema in host.proto
An easy way to populate such a topic is by running:
    produce_messages.py {kafka_topic} proto

This script assumes the nodes with the label :Customer will eventually become supernodes, and times
batches of queries against an arbitrary :Customer node. The queries being timed are designed to
paginate 100 edges at a time through the entire set of nodes adjacent to a :Customer supernode.

Total time to query nodes adjacent to a supernode should slow proportional to the number of edges on
the supernode. The Quine cluster should remain stable.
"""

kafka_topic = "hosts-proto"
kafka_reset = "earliest"
group_id = f"b6-{Random().randint(1, 10*1000)}"
page_size = 100

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


def paginated_query(customer_id: str, page: int):
    page_start = page_size * page
    return f"MATCH (n)-[e]-(m) WHERE strId(n) = '{customer_id}' RETURN m SKIP {page_start} LIMIT {page_size}"


def run_b6():
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
            # There is only 1 result because of the LIMIT 1
            assert(len(query_results) == 1)
            # First column of the first result
            known_customer_id = query_results[0][0]
            print(
                f"Found a customer to monitor: (stringified) id is '{known_customer_id}'")

    while True:
        sleep(verification_poll_interval_sec)
        results_so_far = 0
        page = 0
        # To paginate over a well-defined collection of edges, pin the timestamp
        pinned_time_ms = int(time()*1000)
        print("Starting a paginated query batch")
        query_batch_start_time = perf_counter()
        while True:  # repeatedly query until we get a partial page
            page_start_time = perf_counter()
            query = paginated_query(known_customer_id, page)
            api_result = requests.post(f"{a_quine_host}/api/v1/query/cypher", params={
                "at-time": pinned_time_ms
            }, json={
                "text": query,
                "parameters": {}
            }).json()
            assert(api_result["columns"] == ["m"])
            result_count = len(api_result["results"])
            page_end_time = perf_counter()
            # print(f"Page {page} took {(page_end_time-page_start_time)*1000:,.1f} milliseconds")
            page += 1
            results_so_far += result_count
            if result_count < page_size:
                # we got less than a full page worth of results: we've completed pagination!
                break
            elif page % 10 == 0 and (perf_counter() - query_batch_start_time) > 30:
                # If it's been a while, reassure the user that queries are ongoing
                print(
                    f"Query batch ongoing: Currently on query {page} ({results_so_far} results so far)")
        query_batch_finish_time = perf_counter()
        print(
            f"""Paginated queries returned {results_so_far} nodes in {(query_batch_finish_time-query_batch_start_time):,.2f} seconds (at-time={pinned_time_ms})""")


if __name__ == "__main__":
    if len(sys.argv) > 1 and (sys.argv[1] == "run" or sys.argv[1] == "b6"):
        run_b6()
