from pykafka import KafkaClient


def connect(hosts, topics):
    # create client given hosts
    client = KafkaClient(hosts=hosts)

    # connect to topic and generate sync producer
    topic = client.topics[topics]
    producer = topic.get_sync_producer()

    return client, producer
