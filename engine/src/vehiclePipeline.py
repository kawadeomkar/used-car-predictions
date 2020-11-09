import argparse
import csv
import json
import os
from pykafka import KafkaClient
from pykafka.common import OffsetType
import threading
import boto3
from datetime import datetime

# TODO: this should probably be in a class, configparser for creds/other stuffi

threads = []
# TESTING: add lock for writing to textfile, temporary ML training file
lock = threading.Lock()

parser = argparse.ArgumentParser()
# TODO: Remove defaults (keep for local testing) 
parser.add_argument("--hosts", required=True, default="localhost:9092")
parser.add_argument("--zookeeper", required=True, default="localhost:2181")
parser.add_argument("--topics", required=True, default="vehicle_scraper")
parser.add_argument("--consumer_group", required=True, default="vehicle_pipeline")
parser.add_argument("--threads", required=True, default=4)
arguments = parser.parse_args()

# zookeeper and kafka broker connection
hosts = str(arguments.hosts)
topic = str(arguments.topics)
zookeeper = str(arguments.zookeeper)
consumer_group = str(arguments.consumer_group)
threadCount = int(arguments.threads)


def connectS3():
    # TODO: change profile name
    s3_session = boto3.Session(profile_name="omkar")
    s3 = s3_session.resource('s3')
    bucket = s3.Bucket('craigslist-vehicle-scraper')
    print("created bucket" +bucket.name)
    return bucket


def s3CraigslistSink(bucket, records):
    time = "T".join(str(datetime.now()).split(' '))
    s3Obj = bucket.Object("craigslist-" + datetime.now().strftime("%m-%d-%Y") +  "/" + time + ".json")
    print("sending records to s3")
    # TODO: try catch
    s3Obj.put(
        Body=(bytes(json.dumps(records).encode('UTF-8')))
    )


def connectKafka(hosts, topics, zookeeper, consumer_group):
	
	# create client given hosts
	client = KafkaClient(hosts=hosts)
  
	# only want a single topic
	assert(',' not in topics)

	# connect to topic and generate sync producer, current architure allows new consumers to join
	# at the latest offset from the last consumer, remove auto_offset_reset later since reset_offset_on_start is False
	topic = client.topics[topics]
	consumer = topic.get_balanced_consumer(
		auto_commit_enable=True,
		auto_offset_reset=OffsetType.EARLIEST,
		zookeeper_connect=zookeeper,
		consumer_group=consumer_group,
		reset_offset_on_start=False,
		consumer_timeout_ms=5000
	)
 
	return client, consumer
	


def consume():
    # create new consumer per thread
    print(hosts, topic, zookeeper, consumer_group)
    client, consumer = connectKafka(hosts, topic, zookeeper, consumer_group)
    s3Bucket = connectS3()
    recordCounter = 0
    records = []

    print(type(consumer))
    print(type(consumer.held_offsets))
    print(consumer.held_offsets)

    for msg in consumer:
        record = json.loads(msg.value.decode('utf-8'))
        records.append(record)
        # TODO: hardcoded 1k record count before sink to s3 (lol hardcode fix later)
        if len(records) == 1000:
            print(len(records))
            s3CraigslistSink(s3Bucket, records)
            consumer.commit_offsets()
            records.clear()
        # writeFile(json.loads(msg.value.decode('utf-8')))

    print(len(records))
    if len(records) > 0:
        s3CraigslistSink(s3Bucket, records)
        consumer.commit_offsets()

    consumer.stop()


# temporary saving to local disk for ml training
def writeFile(jsonObj):
	with lock:
		with open("training.csv", 'a', encoding='utf-8') as f:
			f.write(json.dumps(jsonObj))

	
def initThreading():
	#threadDict = threading.local()
	#threadDict.consumer = None
	
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
