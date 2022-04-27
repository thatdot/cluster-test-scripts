#!/usr/bin/python3

import requests
from typing import *
import sys
from time import sleep

"""
Env: Utilities
===========================

This file contains utilities for running Quine cluster tests. It is recommended to run this file
in interactive mode: python -i prodenv.py

"""

"""
data center cluster
http://10.140.183.142:8080/

"""

kafka_reset = "earliest"

protobuf_schema_url = "https://thatdot-public.s3.us-west-2.amazonaws.com/host.desc"


# Hosts that form the cluster. EDIT THIS.

# THATDOT DEVELOPER
quine_hosts: List[str] = ["http://localhost:8080", "http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]
quine_hosts_with_spares: List[str] = quine_hosts + \
    ["http://localhost:8084"]

# if (len(quine_hosts) < 3):
#     print("G2 expects a cluster of at least 3 hosts, please update prodenv.py")
#     exit(-1)

# Sometimes we just need an arbitrary (but consistent) host in the cluster to run an API call against
a_quine_host = quine_hosts[0]

# Number of partitions in the kafka topics
kafka_partitions = 32

# kafka broker string
kafka_servers = "broker0.kafka:9092"

# how many ingest queries to execute simultaneously (per-host)
ingest_parallelism = 32

# seed to use for deterministic data generation
datagen_seed = 1234
# number of messages to generate: used as a secondary seed for data generation and validation
message_count = 1000*1000*1000


def clusterStatus():
    for i, quine_host in enumerate(quine_hosts):
        stats = requests.get(f"{quine_host}/api/v1/admin/status").json()
        print(f"host={quine_host} fullyUp={stats['fullyUp']}")


def clusterStatusAll():
    for i, quine_host in enumerate(quine_hosts_with_spares):
        stats = requests.get(f"{quine_host}/api/v1/admin/status").json()

        print("-------------------- " + quine_host + " --------------------")
        print(f"host={quine_host} fullyUp={stats['fullyUp']}")
        print(stats)
        print("-------------------------------------------------------------")


# Given a host index, retrieve the partitions that host should read from
def partitions(for_host: int) -> List[int]:
    return list(range(for_host, kafka_partitions, len(quine_hosts)))


def expect(context: str, expected, given):
    assert expected == given, f"for {context}, {given} was not {expected}"


def expectContains(context: str, expected: str, given: str):
    assert expected in given, f"for {context}, {given} did not contain {expected}"


def expectConfig(config, key: str, expected):
    expect(key, expected, config[key])


def expectConfigContains(config, key: str, expected: str):
    expectContains(key, expected, config[key])


def checkConfig():
    print("""
    
!!! Ensure your quine.conf includes the following settings: !!!

akka.http.server.idle-timeout=infinite
akka.http.server.parsing.max-content-length=1GB
akka.http.routing.decode-max-size=1GB
akka.remote.artery.advanced.maximum-frame-size = 2 MiB
akka.remote.artery.log-frame-size-exceeding = 128 KiB
datastax-java-driver {
  advanced.throttler {
      class = ConcurrencyLimitingRequestThrottler
      max-concurrent-requests = 3072
      max-queue-size = 30000
    }
}

!!! Ensure your Cassandra configuration includes the following setting: !!!

commitlog_segment_size_in_mb=128

""")
    for quine_host in quine_hosts:
        config = requests.get(
            f"{quine_host}/api/v1/admin/config").json()["quine"]
        store = config["store"]
        persistence = config["persistence"]
        idProvider = config["id"]
        expectConfig(idProvider, "partitioned", True)
        expectConfig(config, "edge-iteration", "reverse-insertion")
        expectConfig(persistence, "journal-enabled", True)
        expectConfigContains(store, "read-consistency", "QUORUM")
        expectConfigContains(store, "write-consistency", "QUORUM")

# aggregate ingest rates across all quine_hosts
# Gets a lower bound on the ingest rate of the cluster


def overallIngestRate(which_ingest: str) -> float:
    totalIngested = 0
    longestMillis = 0
    for quine_host in quine_hosts:
        stats = requests.get(
            f"{quine_host}/api/v1/ingest/{which_ingest}").json()["stats"]
        totalIngested += stats["ingestedCount"]
        longestMillis = max(longestMillis, stats["totalRuntime"])
    return totalIngested / (longestMillis / 1000)


# Prints the full ingest stats block for the named ingest for each host of the cluster
def printIngestStatus(which_ingest: str) -> None:
    for quine_host in quine_hosts:
        try:
            stats = requests.get(
                f"{quine_host}/api/v1/ingest/{which_ingest}").json()["stats"]
            print(stats)
        except Exception as e:
            print(e)


# Prints the full ingest stats block for the named ingest for each host of the cluster
def printIngestedCounts(which_ingest: str) -> None:
    totalCnt = 0
    for quine_host in quine_hosts:
        try:
            stats = requests.get(
                f"{quine_host}/api/v1/ingest/{which_ingest}").json()["stats"]

            totalCnt += stats['ingestedCount']
            print(
                f"host={quine_host} count={stats['ingestedCount']} oneMinRate={stats['rates']['oneMinute']} ")
        except Exception as e:
            print(e)
    print(f"-----------")
    print(f"totalCount={totalCnt}")


def listIngest() -> None:
    for quine_host in quine_hosts_with_spares:
        stats = requests.get(
            f"{quine_host}/api/v1/ingest").json()
        print("-------------------- " + quine_host + " --------------------")
        print(stats)
        print("-------------------------------------------------------------")


def ingestStats(quine_host: str, which_ingest: str):
    stats = requests.get(
        f"{quine_host}/api/v1/ingest/{which_ingest}").json()["stats"]

    return stats


def listAllIngests() -> None:
    one_min_rate = 0
    for quine_host in quine_hosts:
        stats = requests.get(f"{quine_host}/api/v1/ingest").json()
        # print(stats)
        for which_ingest in stats:
            if stats[which_ingest]["status"] == "FAILED":
                print(f"INGEST FAIL ON {quine_host}")
                print(stats)
                continue
            if stats[which_ingest]["status"] == "COMPLETED":
                continue

            stats_indv = ingestStats(quine_host, which_ingest)
            print(
                f"host={quine_host} ingest={which_ingest} count={stats_indv['ingestedCount']} oneMinRate={stats_indv['rates']['oneMinute']} ")
            one_min_rate += stats_indv['rates']['oneMinute']

    print(f"total EPS: {one_min_rate}")


def deleteAllIngest() -> None:
    for quine_host in quine_hosts:
        stats = requests.get(f"{quine_host}/api/v1/ingest").json()
        # print(stats)
        for which_ingest in stats:
            stats = requests.delete(
                f"{quine_host}/api/v1/ingest/{which_ingest}").json()
            print(stats)


def deleteIngest(which_ingest: str) -> None:
    for quine_host in quine_hosts:
        try:
            stats = requests.delete(
                f"{quine_host}/api/v1/ingest/{which_ingest}").json()
            print(stats)
        except Exception as e:
            pass


def addSampleQuery():
    resp = requests.put(f"{a_quine_host}/api/v1/query-ui/sample-queries", json=[
        {
            "name": "Get a few recent nodes",
            "query": "g.recentV(100)"
        }
    ])
    if resp.ok:
        print(f"Added sample query OK on {a_quine_host}")
    else:
        print(f"sample query not set OK on {a_quine_host} " + resp.text)


def clusterHealthAPICall():
    for quine_host in quine_hosts:
        headers = {'Content-Type': 'text/plain', 'accept': 'application/json'}
        resp = requests.post(
            f"{quine_host}/api/v1/query/gremlin", data="g.recentV(10)", headers=headers)
        if resp.ok:
            print(f"Host OK: {quine_host}")
        else:
            print(f"ERROR! Host has issues: {quine_host} " + resp.text)


def removeAllStandingQueries():
    queries = requests.get(
        f"{a_quine_host}/api/v1/query/standing").json()

    for query in queries:
        resp = requests.delete(
            f"{a_quine_host}/api/v1/query/standing/{query['name']}")
        print(f"deleted SQ={query['name']}")
        print(resp)


def startIngests(streams):
    for stream in streams:
        for i, quine_host in enumerate(quine_hosts):
            print(f"stream={stream} host={quine_host}")
            sobj = streams[stream]

            if sobj["format"] == "JSON":
                startIngestJSON(quine_host, partitions(i), sobj)
            else:
                startIngestPROTO(quine_host, partitions(i), sobj)


def startIngestJSON(quine_host, partitions, sobj):
    resp = requests.post(f"{quine_host}/api/v1/ingest/{sobj['name']}", json={
        "type": "KafkaIngest",
        "topics": {
            sobj["topic"]: partitions
        },
        "bootstrapServers": kafka_servers,
        "groupId": "3",
        "autoOffsetReset": sobj['kafka_reset'],
        "parallelism": ingest_parallelism,
        "format": {
            "type": "CypherJson",
            "query": sobj["query"],
            "parameter": "props"
        }
    })
    if resp.ok:
        print(f"Registered JSON ingest={sobj['name']} ec2 on {quine_host}")
    else:
        print(resp.text)
        sys.exit(1)


def startIngestPROTO(quine_host, partitions, sobj):
    resp = requests.post(f"{quine_host}/api/v1/ingest/{sobj['name']}", json={
        "type": "KafkaIngest",
        "topics": {
            sobj["topic"]: partitions
        },
        "bootstrapServers": kafka_servers,
        "groupId": sobj["group_id"],
        "autoOffsetReset": kafka_reset,
        "parallelism": ingest_parallelism,
        "offsetCommitting": {
            "maxBatch": 1000,
            "maxIntervalMillis": 1000,
            "parallelism": 100,
            "waitForCommitConfirmation": False,
            "type": "ExplicitCommit"
        },
        "format": {
            "type": "CypherProtobuf",
            "query": sobj["query"],
            "parameter": "props",
            "schemaUrl": protobuf_schema_url,
            "typeName": sobj["type"]
        }
    })
    if resp.ok:
        print(f"Registered PROTO ingest={sobj['name']} ec2 on {quine_host}")
    else:
        print(resp.text)
        # sys.exit(1)


def register_standing_queries(queries) -> bool:
    ok = True
    for qname, qparts in queries.items():
        resp = requests.post(f"{a_quine_host}/api/v1/query/standing/{qname}", json={
            "pattern": {
                "type": "Cypher",
                "query": qparts["match"]
            },
            "outputs": {
                "toFromStructure": {
                    "type": "CypherQuery",
                    "parameter": "sqMatch",
                    "query": qparts["action"],
                    "parallelism": 32,
                    "executionGuarantee": "AtLeastOnce"
                }
            },
            "bufferCapacity": 100000,
        })
        if resp.ok:
            print(f"Registered standing query {qname} on {a_quine_host}")
        else:
            print(
                f"NOT Registered OK for {qname} on {a_quine_host}: " + resp.text)
            ok = False
    return ok


def print_ingest_streams(streams):
    while True:
        for stream in streams:
            printIngestedCounts(stream)
            print("\n---------\n")
        sleep(3)


def list_kafka_partitions() -> None:
    topic_map = {}
    for quine_host in quine_hosts:
        stats = requests.get(f"{quine_host}/api/v1/ingest").json()
        print(stats)
        for which_ingest in stats:
            print(which_ingest)
            topics = stats[which_ingest]["settings"]["topics"]
            for topic in topics:
                if topic not in topic_map:
                    topic_map[topic] = {}
                for partition in topics[topic]:
                    topic_map[topic][partition] = True

    print(f"topicmap={topic_map}")

    for topic in topic_map:
        tlen = len(topic_map[topic])
        print(f"topic={topic} has {tlen} partitions assigned")
