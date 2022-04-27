from random import Random
from time import sleep
from typing import Dict, List, Tuple
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from google.protobuf import json_format
from kafka.consumer.fetcher import ConsumerRecord
from utils.prodenv import kafka_servers, a_quine_host
from string import ascii_lowercase
from model.host_pb2 import Host
import requests
import json


class DataChecker(object):
    def __init__(self, checks_per_offset_update=20, is_proto: bool = True, kafka_brokers: str = kafka_servers, kafka_topic: str = "hosts", consumer_group: str = "1", quine_url: str = a_quine_host):
        self.is_proto = is_proto
        self.kafka_topic = kafka_topic
        # How many records to check, per partition, per change in committed offset. If this exceeds the size of the committed batch, some records will be re-tested
        self.checks_per_offset_update = checks_per_offset_update
        # This consumer will keep its offsets in sync with the last committed by Quine and therefore must use the same consumer group
        self.commit_sync_consumer = KafkaConsumer(
            bootstrap_servers=kafka_brokers, group_id=consumer_group, enable_auto_commit=False)
        # This consumer will hop the the offsets discovered and read out data to verify
        self.data_read_consumer = KafkaConsumer(
            bootstrap_servers=kafka_brokers, group_id=''.join(Random().choices(ascii_lowercase, k=12)), enable_auto_commit=False
        )
        # Cache partition identities -- these will be frequently referred-to
        self.partitions = [TopicPartition(
            kafka_topic, part) for part in self.commit_sync_consumer.partitions_for_topic(kafka_topic)]
        # Initialize the commit-tracking consumer to track all partitions
        self.commit_sync_consumer.assign(self.partitions)
        self.quine_url = quine_url
        # map of partitions to last checked offset
        self.offset_cache: Dict[TopicPartition, int] = {}

    # Get the most recent not-yet-checked record's raw data for each partition
    def lastest_committed_records(self) -> List[ConsumerRecord]:
        # Force an update of commit offsets with the commit_sync_consumer
        offsets: Dict[TopicPartition, OffsetAndMetadata] = self.commit_sync_consumer._coordinator.fetch_committed_offsets(
            self.partitions)

        data = []
        # for each/some partition[s], retrieve the record at the latest committed offset using the data_read_consumer
        for partition, commit in offsets.items():
            # Don't bother re-reading a message we've already checked
            if self.offset_cache.get(partition) != commit.offset:
                # print(f"Offset cache for {partition} was {self.offset_cache.get(partition)} now {commit.offset}")
                self.offset_cache[partition] = commit.offset

                # read up to self.checks_per_offset_update records, but no further than the last read record
                checks_remaining = min(
                    self.checks_per_offset_update, commit.offset-1)

                # configure the reader
                self.data_read_consumer.assign([partition])
                self.data_read_consumer.seek(
                    partition, commit.offset-(checks_remaining+1))

                # use the for syntax to access data_read_consumer as an iterator
                for record in self.data_read_consumer:
                    checks_remaining -= 1
                    data.append(record)
                    if checks_remaining == 0:
                        break
        return data

    # Get the most recent not-yet-checked record (dictionary form) for each partition
    def verifiable_data(self) -> List[dict]:
        # get the raw records
        records = self.lastest_committed_records()
        data = []
        # parse the raw records to dicts like those used by Datagen
        for record in records:
            if self.is_proto:
                msg = Host()
                msg.ParseFromString(record.value)
                data.append(json_format.MessageToDict(
                    msg, preserving_proto_field_name=True))
            else:
                data.append(json.loads(record.value))
        return data

    """
        Spot check the data integrity of a recent host as committed to Quine.
        This assumes that host node IDs are generated with the following:
            locIdFrom($props.customer_id, 'host', $props.customer_id, $props.entity_id)

        Returned entries are (test result, expected datum, actual datum, query)
    """

    def do_spot_checks(self) -> List[Tuple[bool, dict, dict, str]]:
        expected_data = self.verifiable_data()
        results = []
        for expected_datum in expected_data:
            try:
                # print(expected_datum)
                query = f"""MATCH (n) WHERE id(n) = locIdFrom('{expected_datum["customer_id"]}', 'host', '{expected_datum["customer_id"]}', '{ expected_datum["entity_id"] }') RETURN n"""
                quine_result = requests.post(f"{self.quine_url}/api/v1/query/cypher/nodes", json={
                    "text": query,
                    "parameters": {}
                })
                actual_datum = quine_result.json()[0]["properties"]
                results.append((actual_datum == expected_datum,
                            expected_datum, actual_datum, query))
            except:
                print("An error occurred while running a validation query: This probably indicates a hot-spare is swapping in. Resuming in 10 seconds")
                sleep(10)
        return results
