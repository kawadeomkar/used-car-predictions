from pykafka import KafkaClient
import os

def connect(hosts, topics):
    
    # create client given hosts
    client = KafkaClient(hosts=hosts)
  
    # connect to topic and generate sync producer
    topic = client.topics[topics]
    producer = topic.get_sync_producer()
 
    return client, producer
    


def consume(arguments):
    
    hosts = str(arguments.hosts)
    topic = str(arguments.topics)

    client, producer = connect(hosts, topic)

    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--hosts", required=True, default="localhost:9092")
    parser.add_argument("--topics", required=True, default="vehicleScraper")
    arguments = parser.parse_args()

    consume(arguments)
