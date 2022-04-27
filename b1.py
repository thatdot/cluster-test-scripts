from multiprocessing.connection import wait
from data_checker import DataChecker
from utils.prodenv import *
from string import ascii_letters
import sys
from random import Random
from datagen import Datagen

"""
B1: Standard Data Pipeline (B2: Chaos Monkey variant)

B1-1 [preconfigured] (Recommended Approach):
    1 ingest query
    3 graph-forming Standing Queries, registered before ingest
Run: b1.py b1-1

B1-2 [ongoing configuration]:
    1 ingest query
    3 graph-forming Standing Queries registered over time ({wait_between_sqs_sec} apart)
Run: b1.py b1-2

As with all test scripts, ensure prodenv.py and variables at the top of the script reflect your
current Quine cluster before starting the test.

To run, you will need a kafka topic named {kafka_topic} ("hosts-proto" by default) containing
Protobuf-encoded messages cooresponding to the schema in host.proto
An easy way to populate such a topic is by running:
    produce_messages.py {kafka_topic} proto

Monitor ingest rate via Grafana. Ingest should remain stable and all data should be present
and correctly linked

To perform scenario B2, run whichever B1 variant you wish, and `kill -9` Quine cluster members
according to the scenario specification as follows:
    "Every 15-20 minutes, kill -9 the Quine process on a machine, observe failover to hot spare,
    when cluster is stabilized bring killed node back to act as a hot spare"
To perform scenario B4, run whichever B1 variant you wish, and `kill -9` Quine cluster members
after the ingest has reached 1B events ingested.
"""

kafka_topic = "hosts-proto"
kafka_reset = "earliest"
group_id = f"b1-{Random().randint(1, 10*1000)}"

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

sq_ip_match = """MATCH (n) WHERE exists(n.local_ip)  RETURN DISTINCT id(n) AS id"""
sq_ip_action = (
    """MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
    """MATCH (m) WHERE id(m) = locIdFrom(n.customer_id, 'ip', n.customer_id, n.local_ip) """ +
    """CREATE (n)-[:ipv4]->(m) """ +
    """SET m.ipv4 = n.local_ip, m.customer_id = n.customer_id, m:IPV4"""
)

sq_ou_match = """MATCH (n) WHERE exists(n.ou)  RETURN DISTINCT id(n) AS id"""
sq_ou_action = (
    """MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
    """MATCH (m) WHERE id(m) = locIdFrom(n.customer_id, 'ou', n.customer_id, n.ou) """ +
    """CREATE (n)-[:ou]->(m) """ +
    """SET m.ouname = n.ou, m.customer_id = n.customer_id, m:OU"""
)

sq_site_match = """MATCH (n) WHERE exists(n.site_name)  RETURN DISTINCT id(n) AS id"""
sq_site_action = (
    """MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
    """MATCH (m) WHERE id(m) = locIdFrom(n.customer_id, 'sitename', n.customer_id, n.site_name) """ +
    """CREATE (n)-[:site]->(m) """ +
    """SET m.site_name = n.site_name, m.customer_id = n.customer_id, m:SITE"""
)

all_standing_queries = {
    "ipv4_v2": {
        "match": sq_ip_match,
        "action": sq_ip_action,
    },
    "ou_v2": {
        "match": sq_ou_match,
        "action": sq_ou_action,
    },
    "site_v2": {
        "match": sq_site_match,
        "action": sq_site_action,
    }
}


def run_checker():
    print("Starting data integrity checker -- comparing Kafka records to data retrieved from Quine")
    checker = DataChecker(checks_per_offset_update=20,
                          is_proto=True,
                          kafka_brokers=kafka_servers,
                          kafka_topic=kafka_topic,
                          consumer_group=group_id,
                          quine_url=a_quine_host)
    checks = 0
    errors = 0
    while True:
        spot_check_results = checker.do_spot_checks()
        for success, expected, actual, query in spot_check_results:
            checks += 1
            if not success:
                errors += 1
                print(
                    f"""Spot-checker found an error: Expected host: {expected} but Quine returned host: {actual}. Query: "{query}" """)
        if spot_check_results:
            print(
                f"Checked: {checks} Errors: {errors} ({(errors/checks*100):,.0f}%)")
        sleep(2)


def run_b1_1():
    checkConfig()
    register_standing_queries(all_standing_queries)
    startIngests(ingest_streams)
    run_checker()


def run_b1_2():
    checkConfig()
    startIngests(ingest_streams)
    for sq_name, sq_definition in all_standing_queries.items():
        print(
            f"Waiting {wait_between_sqs_sec} seconds before registering Standing Query {sq_name}")
        sleep(wait_between_sqs_sec)
        register_standing_queries({
            sq_name: sq_definition
        })
    run_checker()


if __name__ == "__main__":
    if len(sys.argv) > 1 and (sys.argv[1] == "run" or sys.argv[1] == "b1" or sys.argv[1] == "b1-1"):
        run_b1_1()
    if len(sys.argv) > 1 and sys.argv[1] == "b1-2":
        run_b1_2()
