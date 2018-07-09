#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import threading
import math
import re
import _pickle as pickle
from queue import Queue
from confluent_kafka import Producer, Consumer, KafkaError
import time
from .electron import Electron
from . import util
import argparse
from .connectors.aerospike import AerospikeConnector
from .connectors.local import LocalConnector
import logging


class LinkQueue(Queue):
    def __init__(self,
                 minimum_messages_to_commit=1,
                 messages_left_to_commit=None):
        if messages_left_to_commit is None:
            messages_left_to_commit = minimum_messages_to_commit

        self.minimum_messages_to_commit = minimum_messages_to_commit
        self.messages_left_to_commit = messages_left_to_commit

        super().__init__(maxsize=0)


class Link:
    ########################## LINK EXECUTION MODES ##########################
    # Multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS = 'MKI'

    # Custom output without multiple kafka inputs
    CUSTOM_OUTPUT = 'CO'

    # Custom input with multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT = 'MKICO'

    # Custom input
    CUSTOM_INPUT = 'CI'
    ##########################################################################

    def __init__(self, log_level='INFO'):
        if log_level == 'NOTSET':
            log_level = logging.NOTSET
        elif log_level == 'DEBUG':
            log_level = logging.DEBUG
        elif log_level == 'INFO':
            log_level = logging.INFO
        elif log_level == 'WARNING':
            log_level = logging.WARNING
        elif log_level == 'ERROR':
            log_level = logging.ERROR
        elif log_level == 'CRITICAL':
            log_level = logging.CRITICAL
        logging.getLogger().setLevel(log_level)

    def _output(self, kafka_producer=True):
        if kafka_producer:
            properties = self.common_properties
            properties.update({
                'partition.assignment.strategy': 'roundrobin',
                'queue.buffering.max.ms': '1'
            })

            # Asynchronous Kafka Producer
            self.producer = Producer(properties)

        self.threads['output']['running'] = True
        while self.threads['output']['running']:
            queue_item = self.queue.get()

            # If a custom input is used, the stored objects are not
            # Kafka Message instances but single Electron instances
            if type(queue_item) is Electron:
                electrons = [queue_item]
            else:
                try:
                    electron = pickle.loads(queue_item.value())
                except Exception:
                    try:
                        electron = \
                            Electron(queue_item.key(),
                                     queue_item.value().decode('utf-8'))
                    except Exception:
                        util.print_exception(self, 'Unsupported serialization.')
                        continue

                # Clean the previous topic
                electron.previous_topic = electron.topic
                electron.topic = None

                # The destiny topic will be overwritten if desired in the
                # transform method (default, first output topic)
                try:
                    electrons = self.transform(electron)
                except Exception:
                    util.print_exception(self, "Exception during the execution of \"transform\". Exiting...", fatal=True)

                if electrons and type(electrons) is not list:
                    electrons = [electrons]

            if not kafka_producer or not electrons:
                continue

            for electron in electrons:
                # The key is enconded for its use as partition key
                partition_key = None
                if electron.key:
                    if type(electron.key) is str:
                        partition_key = electron.key.encode('utf-8')
                    else:
                        partition_key = pickle.dumps(electron.key)

                # Electrons are serialized with pickle
                output = pickle.dumps(electron, protocol=4)

                if electron.topic:
                    output_topic = electron.topic
                # If the destiny topic is not specified, the first is used
                else:
                    output_topic = self.output_topics[0]

                try:
                    self.producer.produce(topic=output_topic,
                                          key=partition_key,
                                          value=output)

                    # Synchronous Writes
                    self.producer.flush()
                except Exception:
                    util.print_exception(self, "Kafka producer error. Exiting...", fatal=True)

    def _kafka_consumer(self, topic_assignments=None):
        if self.mki_mode == 'parity':
            topic_assignments = {-1: -1}
        # If topics are not specified, the first is used
        elif topic_assignments is None:
            topic_assignments = {}
            topic_assignments[self.input_topics[0]] = -1

        # Kafka Consumer
        properties = self.common_properties
        if not self.consumer_group:
            self.consumer_group = __class__.__name__
        properties.update({
            'group.id': self.consumer_group,
            'enable.auto.commit': True,
            'auto.offset.reset': 'smallest'
        })
        consumer = Consumer(properties)
        logging.info(f"Consumer group: {self.consumer_group}")

        prev_queued_messages = 0
        self.threads['input']['running'] = True
        while self.threads['input']['running']:
            for i, topic in enumerate(topic_assignments.keys()):
                consumer.unsubscribe()

                # Buffer for the current topic
                message_buffer = []

                if self.mki_mode == 'exp':
                    subscription = [topic]
                elif self.mki_mode == 'parity':
                    subscription = self.input_topics
                else:
                    logging.error('Unknown mode')
                    return

                consumer.subscribe(subscription)
                try:
                    start_time = util.get_current_timestamp()
                    assigned_time = topic_assignments[topic]

                    while assigned_time == -1 or \
                    Link.in_time(start_time, assigned_time):
                        message = consumer.poll()

                        if not message.key() and not message.value():
                            # logging.debug(f'{self.__class__.__name__ } polling...')
                            if len(subscription) == 1 \
                            or self.mki_mode == 'parity':
                                continue
                            # New topic / restart if there are more topics or
                            # there aren't assigned partitions
                            break

                        if message.error():
                            # End of partition event, not really an error
                            if message.error().code() == \
                            KafkaError._PARTITION_EOF:
                                if len(subscription) == 1 \
                                or self.mki_mode == 'parity':
                                    continue
                                break

                            elif message.error():
                                util.print_error(self, message.error())
                        else:
                            # The message is added to a local list that will be
                            # dumped to a queue for asynchronous processing
                            message_buffer.append(message)
                            current_queued_messages = len(message_buffer)

                            self.queue.messages_left_to_commit = \
                                self.queue.messages_left_to_commit - 1

                            # If there is only one message left, the offset is
                            # committed
                            if self.queue.messages_left_to_commit < 1:
                                for message in message_buffer:
                                    self.queue.put(message)
                                message_buffer = []

                                self.queue.messages_left_to_commit = \
                                    self.queue.minimum_messages_to_commit

                                # Synchronous commit
                                # Only with a reasonable minimum value of
                                # minimum_messages_to_commit
                                # consumer.commit(async=False)

                            # Penalize if only one message was consumed
                            if current_queued_messages > 1 \
                            and current_queued_messages > prev_queued_messages - 2:
                                logging.info(f"Penalized. NPM: {current_queued_messages}")
                                time.sleep(3)
                                break

                            # else...
                            prev_queued_messages = current_queued_messages
                            # Let other consumers retrieve messages
                            # time.sleep(1)

                    # Dump the buffer before changing the subscription
                    for message in message_buffer:
                        self.queue.put(message)

                except Exception as e:
                    util.print_exception(self, "Kafka consumer error. Exiting...")
                    try:
                        consumer.close()
                    except Exception:
                        util.print_exception(self, 'Exception when closing the consumer.', fatal=True)

    def _get_index_assignment(self, window_size, index, elements_no, base=1.7):
        """
        window_size implies a full cycle consuming all the queues with
        priority.
        """
        aggregated_value = .0
        index_value = .0

        # The first element has the biggest value
        reverse_index = elements_no - index - 1

        for index in range(elements_no):
            value = math.pow(base, index)
            if index is reverse_index:
                index_assignment = value
            aggregated_value += value

        return (index_assignment / aggregated_value) * window_size

    def setup(self):
        # Needed since the setup method can be left blank
        pass

    def custom_input(self):
        util.print_error(self, "Void \"custom_input\". Exiting...", fatal=True)

    @staticmethod
    def _thread_target(**kwargs):
        try:
            target = kwargs['target']
            kwargs.pop('target')
            target(**kwargs)
        except Exception:
            util.print_exception(target, f"Exception during the execution of \"{target.__name__}\". Exiting...", fatal=True)

    def restart_input(self):
        self.threads['input']['running'] = False
        input_kwargs, input_thread = self._get_input_opts()
        self.threads['input'] = {'thread': input_thread,
                                   'kwargs': input_kwargs}
        input_thread.start()
        logging.info(self.__class__.__name__ + '\'s input restarted.')

    def start(self, link_mode=None, mki_mode='exp', consumer_group=None):
        self.consumer_group = consumer_group
        self.output_topics = []
        self.producer_options = {}
        self.consumer_options = {}
        self.queue = LinkQueue()

        self.mki_mode = mki_mode
        self.link_mode = link_mode
        self._load_args()

        self.common_properties = {
            'bootstrap.servers': self.kafka_bootstrap_server,
            'compression.codec': 'snappy',
            'api.version.request': True
        }
        try:
            self.aerospike = AerospikeConnector(
                self.aerospike_bootstrap_host,
                self.aerospike_bootstrap_port
            )
        except AttributeError:
            self.aerospike = None

        # Overwritable by a link
        try:
            self.setup()
        except Exception:
            util.print_exception(self, "Exception during the execution of \"setup\". Exiting...", fatal=True)

        # Output
        output_kwargs, output_thread = self._get_output_opts()

        # Input
        input_kwargs, input_thread = self._get_input_opts()

        # Start threads
        self.threads = {'output': {'thread': output_thread},
                          'input': {'thread': input_thread,
                                    'kwargs': input_kwargs} }
        output_thread.start()
        input_thread.start()
        logging.info(self.__class__.__name__ + ' link started.')

    def _get_output_opts(self):
        # Disable Kafka producer for custom output modes
        kafka_producer = True
        if self._custom_output():
            kafka_producer = False

        output_kwargs = {'target': self._output,
                         'kafka_producer': kafka_producer}
        output_thread = threading.Thread(target=Link._thread_target,
                                         kwargs=output_kwargs)
        return output_kwargs, output_thread

    def _get_input_opts(self):
        input_target = self._kafka_consumer  # Default Kafka input
        input_kwargs = {}

        # Custom input
        if self._custom_input():
            input_target = self.custom_input

        # Multiple Kafka input topics
        elif self._multiple_kafka_input():
                # Exponential window assignment
                if self.mki_mode == 'exp':
                    input_kwargs['topic_assignments'] = self._get_topic_assignments()

        input_kwargs['target'] = input_target
        input_thread = threading.Thread(target=Link._thread_target,
                                        kwargs=input_kwargs)
        return input_kwargs, input_thread

    def _custom_output(self):
        return self.link_mode == Link.CUSTOM_OUTPUT \
        or self.link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT

    def _custom_input(self):
        return self.link_mode == Link.CUSTOM_INPUT

    def _kafka_input(self):
        return self._multiple_kafka_input() or not self.link_mode

    def _kafka_output(self):
        return not self._custom_output()

    def _multiple_kafka_input(self):
        return self.link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT \
        or self.link_mode == Link.MULTIPLE_KAFKA_INPUTS

    def _get_topic_assignments(self):
        topic_assignments = {}
        window_size = 900  # in seconds, 15 minutes
        topics_no = len(self.input_topics)
        logging.info("Input topics time assingments:")
        for i, topic in enumerate(self.input_topics):
            topic_assingment = \
                self._get_index_assignment(window_size, i, topics_no)
            topic_assignments[topic] = topic_assingment
            logging.info(' - ' + topic + ": " + str(topic_assingment) +
                  " seconds")
        return topic_assignments

    @staticmethod
    def in_time(start_time, assigned_time):
        return (start_time - util.get_current_timestamp()) < assigned_time

    def load_object(self, object_name):
        try:
            if self.resources_location == 'aerospike':
                obj = self.aerospike.get_and_close(
                    object_name,
                    self.aerospike_resources_namespace,
                    self.aerospike_resources_set
                )
                return obj
            elif self.resources_location == 'local':
                lc = LocalConnector(self.local_resources_path)
                return lc.get_object(object_name)
        except Exception:
            util.print_exception(self,
                'Missing resources_location attribute. Exiting...', fatal=True)

    def _load_args(self):
        parser = argparse.ArgumentParser()
        # Input topic
        parser.add_argument('-i',
                            '--input-topic',
                            action="store",
                            dest="input_topics",
                            help='Kafka input topics. Several topics '
                            + 'can be specified separated by commas.',
                            # Required if the link mode is not of type CUSTOM_INPUT
                            required=not self._custom_input)
        # Output topic
        parser.add_argument('-o',
                            '--output-topics',
                            action="store",
                            dest="output_topics",
                            help='Kafka output topics. Several topics '
                            + 'can be specified separated by commas.',
                            # Required if the link mode is not of type CUSTOM_OUTPUT
                            required=not self._custom_output)

        # Kafka bootstrap server
        parser.add_argument('-b',
                            '--kafka-bootstrap-server',
                            action="store",
                            dest="kafka_bootstrap_server",
                            help='Kafka bootstrap server. \
                            I.e., "localhost:9092".',
                            required=True)

        # Aerospike bootstrap server
        parser.add_argument('-a',
                            '--aerospike-bootstrap-server',
                            action="store",
                            dest="aerospike_bootstrap_server",
                            help='Aerospike bootstrap server. \
                            I.e., "localhost:3000".',
                            required=False)
        # Aerospike path
        parser.add_argument('-p',
                            '--resources-location',
                            action="store",
                            dest="resources_location",
                            help='Path for setup resources. \
                            I.e., "aerospike:namespace:set".',
                            required=False)

        args = parser.parse_args()

        # If no commas, a list with 1 element is returned
        # Input topics
        if args.input_topics:
            self.input_topics = args.input_topics.split(',')
        else:
            self.input_topics = None

        # Output topics
        # self.queues = []
        if args.output_topics:
            self.output_topics = args.output_topics.split(',')
            # for topic in self.output_topics:
            #     self.queues.append(LinkQueue())
        else:
            self.output_topics = None

        self.kafka_bootstrap_server = args.kafka_bootstrap_server

        if args.aerospike_bootstrap_server is not None:
            as_bootstrap = args.aerospike_bootstrap_server.split(':')
            self.aerospike_bootstrap_host = as_bootstrap[0]
            self.aerospike_bootstrap_port = int(as_bootstrap[1])

        if args.resources_location is not None:
            resources_location = args.resources_location.split(':')
            self.resources_location = resources_location[0]

            if self.resources_location == 'aerospike':
                self.aerospike_resources_namespace = resources_location[1]
                self.aerospike_resources_set = resources_location[2]
            elif self.resources_location == 'local':
                self.local_resources_path = resources_location[1]
