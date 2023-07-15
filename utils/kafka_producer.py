#!/usr/bin/python
# -*- coding:UTF-8 -*-
from kafka import KafkaProducer
from kafka.errors import kafka_errors
import traceback
import json


def kafka_producer_demo(rows):
    # 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda k: json.dumps(k, ensure_ascii=False).encode(),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    # 发送三条消息
    for row in rows:
        future = producer.send(
            topic='crawl',
            # key='count_num',  # 同一个key值，会被送至同一个分区
            value=row)  # 向分区1发送消息
        print("send {}".format(row))
        try:
            future.get(timeout=10)  # 监控是否发送成功
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()


