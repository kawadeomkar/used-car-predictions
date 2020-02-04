import argparse
import json
import os
from pykafka import KafkaClient
from pykafka.common import OffsetType
import threading

threads = []
# TESTING: add lock for writing to textfile, temporary ML training file
lock = threading.Lock()

parser = argparse.ArgumentParser()
# TODO: Remove defaults (keep for local testing) 
parser.add_argument("--hosts", required=True, default="localhost:9092")
parser.add_argument("--zookeeper", required=True, default="localhost:2181")
parser.add_argument("--topics", required=True, default="vehicleScraper")
parser.add_argument("--consumer_group", required=True, default="vehicle_pipeline")
parser.add_argument("--threads", required=True, default=4)
arguments = parser.parse_args()

hosts = str(arguments.hosts)
topic = str(arguments.topics)
zookeeper = str(arguments.zookeeper)
consumer_group = str(arguments.consumer_group)
threadCount = int(arguments.threads)


def connect(hosts, topics, zookeeper, consumer_group):
	
	# create client given hosts
	client = KafkaClient(hosts=hosts)
  
	# only want a single topic
	assert(',' not in topics)

	# connect to topic and generate sync producer, current architure allows new consumers to join
	# at the latest offset from the last consumer, remove auto_offset_reset later since reset_offset_on_start is False
	topic = client.topics[topics]
	consumer = topic.get_balanced_consumer(
		auto_commit_enable=True,
		auto_offset_reset=OffsetType.LATEST,
		zookeeper_connect=zookeeper,
		consumer_group=consumer_group,
		reset_offset_on_start=False
	)
 
	return client, consumer
	


def consume():
	# create new consumer per thread	
	client, consumer = connect(hosts, topic, zookeeper, consumer_group)
   
	for msg in consumer:
		# TODO connect and send to cassandra
		writeFile(json.loads(msg.decode('utf-8'))) 


# temporary saving to local disk for ml training
def writeFile(jsonObj):
	with lock:
		with open("training.json", 'w', encoding='utf-8') as f:
			json.dump(jsonObj, f, indent=4)

	
def initThreading():
	threadDict = threading.local()
	threadDict.consumer = None
	
	for i in range(threadCount):
		t = threading.Thread(target=consume)
		t.daemon = True
		t.start()
		threads.append(t)


def stopThreads():
	for t in threads:
		t.join()
		

def main():
	initThreading()
	stopThreads()

if __name__ == '__main__':
	main()
