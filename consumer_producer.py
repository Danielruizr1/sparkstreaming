import json
import pykafka


def consume():
    for message in consumer:
        if message is not None:
            print(message.offset, message.value)



if __name__ == "__main__":
    client = pykafka.KafkaClient(hosts="127.0.0.1:9092")
    topic = client.topics[b'udacity.sfcrime']
    consumer = topic.get_simple_consumer()
    consume()
    