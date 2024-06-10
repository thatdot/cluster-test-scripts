from utils.prodenv import *
from string import ascii_letters
import sys
from random import Random
# from kafka import KafkaProducer
from datagen import Datagen
import json
from datetime import datetime
import time
# import itertools

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
group_id = f"b3-{int(time.time() * 1000)}"

# namespace_name = "default"

host_ingest_query = (
    """WITH idFrom($props.customer_id, 'host', $props.customer_id, $props.entity_id) AS hId """ +
    """MATCH (n) WHERE id(n) = hId """ +
    """SET n = $props, n:host """
)
# num_ingest = {
#     "type": "NumberIteratorIngest",
#     "format": {
#         "type": "CypherLine",
#         "query": "MATCH (n) WHERE id(n) = id(gen.node.from(toInteger($that))) SET n:Number, n.i=toInteger($that)",
#     },
#     "maximumPerSecond": 1,
#     "ingestLimit": 1000,
# }
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
wait_between_sqs_sec = 10

r = Random(datagen_seed)


def query(pattern: str):
    return {
        "match": f"MATCH (n) WHERE n.hostname =~ '^{pattern}.*' RETURN DISTINCT id(n)",
        "action": (f"""MATCH (n) WHERE id(n) = $sqMatch.data.id """ +
                   f"""MATCH (m) WHERE id(m) = idFrom(n.customer_id, n.customer_id, '{pattern}') """ +
                   f"""CREATE (n)-[:{pattern}]->(m) """ +
                   f"""SET m.name = "{pattern} BAZ", m:bar""")
    }


def register_query_for_pattern(pattern: str) -> bool:
    return register_standing_queries({
        f"{pattern}": query(pattern),
    })


def nextJson(dgen) -> bytes:
	return json.dumps(dgen.next()).encode('utf-8')


WAIT_TIME_AFTER_REMOVING_QUERIES = 5 * 60
CONTROL_RUN_TIME = 10 * 60
WAIT_TIME_AFTER_ADDING_QUERY = 60
SINGLE_QUERY_RUNTIME = 10 * 60
MULT_QUERY_NUM = 300
WAIT_TIME_AFTER_ADDING_INGEST = 5 * 60
MULTI_QUERY_WAIT_TIME_AFTER = 3 * 60
QUERY_THEN_INGEST_WAIT = 5 * 60
QUERY_THEN_INGEST_RUNTIME = 10 * 60

STRESS_TEST_CONSECUTIVE_FAILURES = 3
STRESS_TEST_WAIT_TIME = .5

def nowStr():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
def printNow(s):
    print(nowStr(), s)

def mkPattern():
    return ''.join(r.choice(ascii_letters) for _ in range(12))

def resetTests():
    printNow("Resetting tests")
    deleteAllIngest()
    removeAllStandingQueries()
        
    sleep(WAIT_TIME_AFTER_REMOVING_QUERIES)
    # printNow("Tests successfully reset")

    # delete_namespace(namespace_name)
    # create_namespace(namespace_name)
    # printNow("Namespace cleared")

def runControl():
    printNow("starting control test")
    startIngests(ingest_streams)
    sleep(CONTROL_RUN_TIME)
    printNow("control test finished")

def runNQueries(n, waitTime):
    for i in range(n):
        start = time.time()
        queryAccepted = register_query_for_pattern(mkPattern())
        printNow("Adding query took " + str(time.time() - start) + " seconds")
        if not queryAccepted:
            printNow("query was not accepted")
        sleep(waitTime)

def runSingleQueryTest():
    printNow("Starting single query test")
    startIngests(ingest_streams)
    sleep(WAIT_TIME_AFTER_ADDING_INGEST)
    runNQueries(1, 0)
    sleep(SINGLE_QUERY_RUNTIME)
    printNow("Single query test done")

def runMultiQueryTest():
    printNow("Starting multi query test")
    startIngests(ingest_streams)
    sleep(WAIT_TIME_AFTER_ADDING_INGEST)
    runNQueries(MULT_QUERY_NUM, WAIT_TIME_AFTER_ADDING_QUERY)
    sleep(MULTI_QUERY_WAIT_TIME_AFTER)
    printNow("Multi query test done")

def runQueriesThenIngest():
    printNow("Starting query for adding ingest then adding query")
    runNQueries(MULT_QUERY_NUM, 1)
    sleep(QUERY_THEN_INGEST_WAIT)
    printNow("Queries added. Starting Ingest")
    startIngests(ingest_streams)    
    sleep(QUERY_THEN_INGEST_RUNTIME)
    printNow("Query then ingest test done")


def run_b3():
    performanceTests()

def runStressTest(withIngest):
    resetTests()
    print("Running stress test", "with ingest" if withIngest else "without ingest")
    nFailures = 0
    nSuccees = 0
    if withIngest:
        startIngests(ingest_streams)    
        sleep(10)
    while True:
        start = time.time()
        queryAccepted = register_query_for_pattern(mkPattern())
        printNow("Adding query took " + str(time.time() - start) + " seconds")
        if not queryAccepted:
            printNow("query was not accepted")
            nFailures += 1
        else:
            nFailures = 0
            nSuccees += 1
        sleep(STRESS_TEST_WAIT_TIME)
        if(nFailures > STRESS_TEST_CONSECUTIVE_FAILURES):
            printNow("Exceeded maximum consecutive failures at " + str(nSuccees) + " exiting")
            break




def performanceTests():
    resetTests()
    runControl()

    resetTests()
    runSingleQueryTest()

    resetTests()
    runControl()

    resetTests()
    runMultiQueryTest()

    resetTests()
    runControl()

    resetTests()
    runQueriesThenIngest()

    resetTests()
    runControl()

    resetTests()
    runControl()

    resetTests()
    runStressTest(True)

    resetTests()
    runControl()

    resetTests()
    runStressTest(False)    

    resetTests()
    runControl()
    """
    #checkConfig()
    removeAllStandingQueries()
    deleteAllIngest()

    # sleep(10)
    startIngests(ingest_streams)
    print("Waiting 1 minute to get a baseline 1-minute ingest rate")
    sleep(60)
    total = 0
    for i in range(1):
        pattern = ''.join(r.choice(ascii_letters) for _ in range(12))
        queryAccepted = register_query_for_pattern(pattern)
        if not queryAccepted:
             total = i-1
             break
        # sleep(wait_between_sqs_sec)
    print("Server rejected the most recent Standing Query", total)
    # dgen = Datagen(datagen_seed, message_count)

    # queryAccepted = True
    # while queryAccepted:
    """

if __name__ == "__main__":
    if len(sys.argv) > 1 and (sys.argv[1] == "run" or sys.argv[1] == "b3"):
        run_b3()
