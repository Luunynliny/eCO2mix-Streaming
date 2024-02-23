import json
import time

from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def send_to_kafka(data):
    topic = "eco2mix-national-tr"
    producer = Producer({"bootstrap.servers": "broker:29092"})

    for d in data:
        producer.produce(
            topic,
            key=d["date_heure"],
            value=json.dumps(d).encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.flush()

        time.sleep(5)
