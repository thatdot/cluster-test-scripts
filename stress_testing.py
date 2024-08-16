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
To set up the environment:

#install
kafka
influxdb (v1)
java
docker
pipenv
script

To run the tests Each in their own terminal (tmux recommended)

#kafka (python host)
docker-compose up


#cassandra (python host)
docker run -it --rm -p9042:9042 --name cassandra cassandra


#before every run
dcqlsh
    drop keyspace quine;

#quine for profiler
cd quine
java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9000 \
    -Dcom.sun.management.jmxremote.rmi.port=9000 -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=localhost \
    -Dquine.help-make-quine-better=false -Dconfig.file=[the quine.conf path here] \
    -Dquine.id.type=uuid \
    -Dquine.id.partitioned=true \
    -jar quine-enterprise-assembly-1.6.4-66-g4ffd0df06.jar 

@quine without profiler
java -Dquine.help-make-quine-better=false \
    -Dconfig.file=quine.conf -Xmx4012m -Xms4012m \
    -Dquine.id.type=uuid \
    -Dquine.id.partitioned=true \
    -jar quine-enterprise-assembly-1.6.4-66-g4ffd0df06.jar 

        
#produce messages (python host)
cd [cluster-testing-directory]
pipenv shell
python produce_messages.py hosts-proto proto

#python tests
cd quine/cluster-test-scripts/
pipenv shell
script
python stress_testing.py run
"""


kafka_topic = "hosts-proto"
kafka_reset = "earliest"
group_id = f"b3-{int(time.time() * 1000)}"

# namespace_name = "default"

host_ingest_query = (
    """WITH locIdFrom(kafkaHash($props.customer_id), 'host', $props.customer_id, $props.entity_id) AS hId """ +
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


WAIT_TIME_AFTER_REMOVING_QUERIES = 0 * 60
CONTROL_RUN_TIME = 10 * 60
WAIT_TIME_AFTER_ADDING_QUERY = 10
SINGLE_QUERY_RUNTIME = 10 * 60
MULT_QUERY_NUM = 1000
WAIT_TIME_AFTER_ADDING_INGEST = 10 #0 * 60
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
        t = time.time()
        # if i == 2 or i == 30:
        #     input("Do the heap dump")
        liveness_check()
        start = time.time()
        queryAccepted = register_query_for_pattern(mkPattern())
        printNow("Adding " + str(i+1) + "th query took " + str(time.time() - start) + " seconds")
        if not queryAccepted:
            printNow("query was not accepted")
        sleep(waitTime - (time.time() - t))

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



tests = {
    "control":runControl,
    "singleQuery":runSingleQueryTest,
    "multiQuery":runMultiQueryTest,
    "queriesThenIngest":runQueriesThenIngest,
    "stressTestWithIngest": lambda : runStressTest(True),
    "stressTestNoIngest": lambda : runStressTest(False),
}
def runTest(testName):
    resetTests()
    if testName in tests:
        print("Running test:", testName)
        tests[testName]()
    else:
        print("Invalid test:", testName)


def performanceTests():
    # resetTests()
    # runControl()

    # resetTests()
    # runSingleQueryTest()

    # resetTests()
    # runControl()

    resetTests()
    runMultiQueryTest()

    # resetTests()
    # runControl()

    # resetTests()
    # runQueriesThenIngest()

    # resetTests()
    # runControl()

    # resetTests()
    # runControl()

    # resetTests()
    # runStressTest(True)

    # resetTests()
    # runControl()

    # resetTests()
    # runStressTest(False)    

    # resetTests()
    # runControl()
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
    if len(sys.argv) > 1:
        if (sys.argv[1] == "run" or sys.argv[1] == "b3"):
            run_b3()
        elif sys.argv[1] in tests:
            runTest(sys.argv[1])
        else:
            print("Invalid test name:", sys.argv[1])
