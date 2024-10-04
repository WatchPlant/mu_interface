#!/usr/bin/env python3
import zmq
import time
import logging

class ZMQ_Publisher_Throttled():

    def __init__(self, min_time=3*60*60):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect("tcp://127.0.0.1:5556")
        time.sleep(1)

        self.last_publish = dict()
        self.min_time = min_time

    def publish(self, message, topic="default"):
        if self.last_publish.get(topic, None) is None or time.time() - self.last_publish[topic] > self.min_time:
            self.last_publish[topic] = time.time()
            self.socket.send_string(message)
            logging.debug(f"Sensor node notifier - published message: {message} on topic: {topic}")