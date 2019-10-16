# -*- coding: utf-8 -*-

"""
Contains RabbitMQ Consumer class
Use under MIT License
"""

__author__ = 'G_T_Y'
__license__ = 'MIT'
__version__ = '1.0.0'


import json
import threading
import pika
import logging

from . import settings
from .exceptions import RabbitmqConnectionError
from .handler import handler
from .publisher import Publisher
from .logger import LOGGER


class Consumer:
    """Used to consume message of request queue"""

    def __init__(self):
        self._host = settings.RABBITMQ_HOST
        self._vhost = settings.RABBITMQ_VHOST
        self._username = settings.RABBITMQ_USERNAME
        self._password = settings.RABBITMQ_PASSWORD
        self._port = settings.RABBITMQ_PORT
        self._request_queue = settings.REQUEST_QUEUE
        self.connection = None
        self.channel = None

    def connect(self):
        """Establish connection to rabbitmq server using parameters set by init function
        It update values of connection and channel parameters
        """

        if settings.DEBUG:
            parameters = pika.ConnectionParameters(self._host)
        else:
            credentials = pika.PlainCredentials(
                username=settings.RABBITMQ_USERNAME,
                password=settings.RABBITMQ_PASSWORD
            )
            parameters = pika.ConnectionParameters(
                host=self._host,
                port=self._port,
                virtual_host=self._vhost,
                credentials=credentials
            )

        try:
            msg = "Connection established successfully with rabbitmq server !!!"
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            print(msg)
            LOGGER.info(msg)
        except Exception as e:
            raise RabbitmqConnectionError(str(e))

    @staticmethod
    def on_message(channel, method_frame, header_frame, body):
        """ Method called when a message is receive on request queue
        convert message to dictionary and launch handler function
        :param channel:
        :param method_frame:
        :param header_frame:
        :param body:
        """
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        message = body.decode("utf8")
        print(message)
        LOGGER.info(message)

        try:
            data = json.loads(message)
            threading.Thread(target=handler, args=(data, )).start()
        except json.JSONDecodeError:
            error = "Invalid JSON file"
            print(error)
            publisher = Publisher()
            publisher.send_message(error)
            # send message to response queue
            # log error

    def run(self):
        """
        Start consuming messages on request queue
        :return:
        """
        self.channel.queue_declare(self._request_queue)
        self.channel.basic_consume(self._request_queue, self.on_message)
        try:
            msg = "Waiting for message ..."
            print(msg)
            LOGGER.info(msg)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

        self.connection.close()
