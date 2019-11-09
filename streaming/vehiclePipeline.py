import argparse
from pykafka import KafkaClient
from pykafka.common import OffsetType
import os

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
	


def consume(arguments):
	
	hosts = str(arguments.hosts)
	topic = str(arguments.topics)
	zookeeper = str(arguments.zookeeper)
	consumer_group = str(arguments.consumer_group)

	client, consumer = connect(hosts, topic, zookeeper, consumer_group)
   
	for msg in consumer:
		# TODO connect and send to cassandra
		print(json.loads(msg.decode('utf-8'))) 
	


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	# TODO: Remove defaults (keep for local testing) 
	parser.add_argument("--hosts", required=True, default="localhost:9092")
	parser.add_argument("--zookeeper", required=True, default="localhost:2181")
	parser.add_argument("--topics", required=True, default="vehicleScraper")
	parser.add_argument("--consumer_group", required=True, default="vehicle_pipeline")
	arguments = parser.parse_args()

	consume(arguments)
