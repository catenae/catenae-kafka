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

# LinkHelper
import argparse
from .connectors.aerospike import AerospikeConnector
from .connectors.local import LocalConnector


class LinkQueue(Queue):
    def __init__(self,
                 minimum_messages_to_commit=1,
                 messages_left_to_commit=None):
        if messages_left_to_commit is None:
            messages_left_to_commit = minimum_messages_to_commit

        self.minimum_messages_to_commit = minimum_messages_to_commit
        self.messages_left_to_commit = messages_left_to_commit

        super().__init__(maxsize=0)


class Link(object):

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

    # def args(self, function):
    #     def wrapper():
    #         self._load_args()
    #         function()
    #     return wrapper

    def _kafka_producer(self):
        properties = self.common_properties
        properties.update({
            'partition.assignment.strategy': 'roundrobin',
            'queue.buffering.max.ms': '1'
        })

        # Asynchronous Kafka Producer
        self.producer = Producer(properties)
        running = True
        while(running):
            queue_item = self.queue.get()

            # If a custom input is used, the stored objects are not
            # Kafka Message instances but single Electron instances
            if type(queue_item) is Electron:
                electrons = [queue_item]

            # Kafka Producer
            else:
                try:
                    electron = pickle.loads(queue_item.value())
                except:
                    try:
                        electron = \
                            Electron(queue_item.key(),
                                     queue_item.value().decode('utf-8'))
                    except:
                        util.print_exception(self, 'Unsupported serialization.')
                        continue

                # Clean the previous topic
                electron.previous_topic = electron.topic
                electron.topic = None

                # The destiny topic will be written if desired in the
                # transform method
                try:
                    electrons = self.transform(electron)
                except Exception as e:
                    util.print_error(self, "Link error. Exiting...", fatal=True)

                if electrons and type(electrons) is not list:
                    electrons = [electrons]

            if not electrons:
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
                    util.print_error(self, "Kafka producer error. Exiting...", fatal=True)

    def _kafka_consumer(self, topic_assignments=None):
        if self.mki_mode == 'parity':
            topic_assignments = {-1: -1}
        # If topics are not specified, the first is used
        elif topic_assignments is None:
            topic_assignments = {}
            topic_assignments[self.input_topics[0]] = -1

        # Kafka Consumer
        properties = self.common_properties
        properties.update({
            'group.id': self.__class__.__name__,
            'enable.auto.commit': True,
            'auto.offset.reset': 'smallest'
        })

        consumer = Consumer(properties)

        # Output queue
        # queue = self.queues[0]

        prev_queued_messages = 0

        running = True
        while running:
            for i, topic in enumerate(topic_assignments.keys()):

                consumer.unsubscribe()

                # Buffer for the current topic
                message_buffer = []

                if self.mki_mode == 'exp':
                    subscription = [topic]
                elif self.mki_mode == 'parity':
                    subscription = self.input_topics
                else:
                    print('Unknown mode')
                    return

                consumer.subscribe(subscription)
                # print(len(consumer.assignment()))

                try:
                    start_time = util.get_current_timestamp()
                    assigned_time = topic_assignments[topic]

                    while assigned_time == -1 or \
                    Link.in_time(start_time, assigned_time):
                        message = consumer.poll()

                        if not message.key() and not message.value():
                            print("Poll...")
                            if len(subscription) == 1 \
                            or self.mki_mode == 'parity':
                                continue
                            # New topic / restart if there are more topics or
                            # there aren't assigned partitions
                            break

                        # print(message.key())
                        # print(message.value())

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
                            # print(pickle.loads(message.value()).value)

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
                                print(f"Penalized. NPM: {current_queued_messages}")
                                time.sleep(10)
                                break

                            # else...
                            prev_queued_messages = current_queued_messages
                            # Let other consumers retrieve messages
                            # time.sleep(1)

                    # Dump the buffer before changing the subscription
                    for message in message_buffer:
                        self.queue.put(message)

                except Exception:
                    util.print_exception(self, 'Unknown exception.')
                    try:
                        consumer.close()
                    except Exception:
                        pass
                    raise SystemExit


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
        pass

    def start(self, link_mode=None, mki_mode='exp'):
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
        self.setup()

        # A) OUTPUT
        output_target = self._kafka_producer  # Default Kafka output
        output_kwargs = {}

        # Deprecated, always use transform method
        # Custom output
        # if self.link_mode is self.KAFKA_INPUT_CUSTOM_OUTPUT:
        #    output_target = self.custom_output

        threading.Thread(target=output_target,
                         kwargs=output_kwargs).start()

        # B) INPUT
        input_target = self._kafka_consumer  # Default Kafka input
        input_kwargs = {}

        # Custom input
        if self.link_mode == self.CUSTOM_INPUT:
            input_target = self.custom_input

        # Multiple Kafka input topics
        elif self.link_mode == self.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT \
                or self.link_mode == self.MULTIPLE_KAFKA_INPUTS:
            # Exponential window assignment
            if self.mki_mode == 'exp':
                window_size = 900  # in seconds, 15 minutes

                topics_no = len(self.input_topics)
                topic_assignments = {}
                print("Input topics time assingments:")
                for i, topic in enumerate(self.input_topics):
                    topic_assingment = \
                        self._get_index_assignment(window_size, i, topics_no)
                    topic_assignments[topic] = topic_assingment
                    print(' - ' + topic + ": " + str(topic_assingment) +
                          " seconds")
                input_kwargs['topic_assignments'] = topic_assignments

        threading.Thread(target=input_target,
                         kwargs=input_kwargs).start()

        print(self.__class__.__name__ + ' link started.')

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
        except:
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
                            # Required if the link mode is not of type
                            # CUSTOM_INPUT
                            required=self.link_mode != Link.CUSTOM_INPUT and \
                                     self.link_mode != Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT)

        # Output topic
        parser.add_argument('-o',
                            '--output-topics',
                            action="store",
                            dest="output_topics",
                            help='Kafka output topics. Several topics '
                            + 'can be specified separated by commas.',
                            # Required if the link mode is not of type
                            # CUSTOM_OUTPUT
                            required=self.link_mode != Link.CUSTOM_OUTPUT and \
                                     self.link_mode != Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUPUT)

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
