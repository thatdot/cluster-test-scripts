from kafka import KafkaProducer
import random, string, json, requests
from datagen import Datagen
from sys import argv
from google.protobuf import json_format
from model.host_pb2 import Host
from utils.prodenv import kafka_servers, datagen_seed, message_count
from kafka.admin import KafkaAdminClient, ConfigResource, NewTopic

"""
Produces Host messages (JSON or protobuf) to the specified kafka topic at the
Kafka cluster configured in utils/prodenv.py
"""

print("""Usage: produce_messages.py <topic> <"proto" | "json">""")

# redpanda topic creation: rpk topic create hosts -p 32 -c retention.ms=-1
# redpanda topic creation: rpk topic create hosts-proto -p 32 -c retention.ms=-1
producer = KafkaProducer(bootstrap_servers=kafka_servers)

topic = argv[1] if len(argv) > 1 else "hosts"
print(f"Producing messages to topic {topic}")
# protoc --python_out=model host.proto
proto = (argv[2].lower() == "proto") if len(argv) > 2 else True
print(f"Protobuf-encoding messages: {proto}")

dgen = Datagen(datagen_seed, message_count)

def nextJson() -> bytes:
	return json.dumps(dgen.next()).encode('utf-8')
def nextProto() -> bytes:
	datum = dgen.next()
	msg = Host()
	json_format.ParseDict(datum, msg)
	return (msg.SerializeToString(), datum["customer_id"])

admin_client = KafkaAdminClient()
# topic_list = []
# topic_list.append(ConfigResource('TOPIC', topic, {"retention.ms":"1000", "retention.bytes":"10000000","segment.bytes":"1000000"}))
# admin_client.alter_configs(topic_list)

admin_client.create_topics([NewTopic(topic, 8, 1, topic_configs={'retention.ms': '-1'})])

for i in range(message_count):
	(payload, partitionKey) = nextProto() if proto else nextJson()
	f = producer.send(topic, key=partitionKey.encode('utf-8'), value=payload)
	if i % 10000 == 0:
		print(f"{i}/{message_count}", flush=True)
		# print(f.get())
		# print(payload)

producer.flush()

print("Done producing into Kafka.")
