#!/usr/bin/python
# -*- coding:UTF-8 -*-
from kafka import KafkaConsumer


def kafka_consumer_demo():
    consumer = KafkaConsumer(
        'paimon_canal_1',
        bootstrap_servers=['node01:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value.decode('utf-8')))

    # consumer = KafkaConsumer(
    #     'kafka_demo',
    #     bootstrap_servers=':9092',
    #     group_id='test'
    # )
    # for message in consumer:
    #     print("receive, key: {}, value: {}".format(
    #         json.loads(message.key.decode()),
    #         json.loads(message.value.decode())
    #     )
    #     )


if __name__ == '__main__':
    kafka_consumer_demo()
