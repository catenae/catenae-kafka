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
from .connectors.mongodb import MongodbConnector
from .connectors.local import LocalConnector
import logging


class LinkQueue(Queue):
    def __init__(self,
                 minimum_messages=1,
                 messages_left=None):
        if messages_left is None:
            messages_left = minimum_messages
        self.minimum_messages = minimum_messages
        self.messages_left = messages_left
        super().__init__(maxsize=-1)


class Link:
    ########################## LINK EXECUTION MODES ##########################
    # Multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS = 'MKI'

    # Custom output without multiple kafka inputs
    CUSTOM_OUTPUT = 'CO'

    # Custom input with multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT = 'MKICO'

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
        logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

        self.lock = threading.RLock()
        self._load_args()

    def _execute_kafka_consumer_commit_callback(self,
        kafka_consumer_commit_callback,
        kafka_consumer_commit_callback_kwargs,
        kafka_consumer_commit_callback_args):

        kafka_consumer_commit_callback_done = False
        kafka_consumer_commit_callback_attempts = 0
        while not kafka_consumer_commit_callback_done:
            if kafka_consumer_commit_callback_attempts == 15:
                util.print_error(self, 'Cannot commit a message. Exiting...', fatal=True)
            try:
                if kafka_consumer_commit_callback_kwargs:
                    kafka_consumer_commit_callback(**kafka_consumer_commit_callback_kwargs)
                elif kafka_consumer_commit_callback_args:
                    kafka_consumer_commit_callback(*kafka_consumer_commit_callback_args)
                else:
                    kafka_consumer_commit_callback()
                kafka_consumer_commit_callback_done = True
            except Exception as e:
                logging.error(f'Trying to commit a message ({kafka_consumer_commit_callback_attempts})...')
                logging.error(str(e))
                if 'UNKNOWN_MEMBER_ID' in str(e):
                    util.print_error(self, 'Cannot commit a message (timeout). Exiting...', fatal=True)
                kafka_consumer_commit_callback_attempts += 1
                time.sleep(2)

    def _output(self, kafka_producer=True):
        if kafka_producer:

            # queue.buffering.max.ms - how long librdkafka will wait for
            # batch.num.messages to be produced by the application before
            # sending them to the broker in a produce request.

            # batch.num.messages - the maximum number of messages that will
            # go in a single produce request to the broker.

            # retries - the client resent the record upon receiving the error.

            # max.in.flight.requests.per.connection - if set to 1, only one
            # request can be sent to the broker at a time, guaranteeing the
            # order if retries is enabled.

            properties = self.common_properties
            properties.update({
                'partition.assignment.strategy': 'roundrobin',
                'retries': 3,
                'queue.buffering.max.ms': 1000,
                'batch.num.messages': 100
            })

            if self.synchronous:
                properties.update({
                    'retries': 5,
                    'max.in.flight.requests.per.connection': 1,
                    'queue.buffering.max.ms': 1,
                    'batch.num.messages': 1
                })

            self.producer = Producer(properties)

        running = True
        while running:
            queue_item = self.queue.get()

            transform_callback = None
            transform_callback_args = None
            transform_callback_kwargs = None

            # If the item is inserted from a custom_input it will be an
            # individual electron
            if type(queue_item) == Electron:
                electrons = [queue_item]

            # If the item comes from the Kafka consumer, it could be an
            # string or a serialized individual electron
            else:
                kafka_consumer_commit_callback = None
                kafka_consumer_commit_callback_args = None
                kafka_consumer_commit_callback_kwargs = None
                if type(queue_item) == tuple:
                    kafka_consumer_commit_callback = queue_item[1]
                    if len(queue_item) > 2:
                        if type(queue_item[2]) == list:
                            kafka_consumer_commit_callback_args = queue_item[2]
                        elif type(queue_item[2]) == dict:
                            kafka_consumer_commit_callback_kwargs = queue_item[2]
                    queue_item = queue_item[0]

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
                    transform_result = self.transform(electron)
                except Exception:
                    util.print_exception(
                        self,
                        'Exception during the execution of "transform". Exiting...',
                        fatal=True)

                if type(transform_result) == tuple:
                    electrons = transform_result[0]
                    # Function to call if asynchronous mode is enabled after
                    # a message is correctly commited to a Kafka broker
                    if len(transform_result) > 1:
                        transform_callback = transform_result[1]
                        if len(transform_result) > 2:
                            if type(transform_result[2]) == list:
                                transform_callback_args = transform_result[2]
                            elif type(transform_result[2]) == dict:
                                transform_callback_kwargs = transform_result[2]
                else:
                    electrons = transform_result

                # Even if no electrons are returned in the transform method,
                # continue so the input electron can be commited by the Kafka
                # consumer (synchronous mode, kafka_output).
                if electrons == None:
                    electrons = []

                # If there is only one electron, convert it to a list
                elif type(electrons) == Electron:
                    electrons = [electrons]

            if kafka_producer:
                for electron in electrons:
                    # The key is enconded for its use as partition key
                    partition_key = None
                    if electron.key:
                        if type(electron.key) == str:
                            partition_key = electron.key.encode('utf-8')
                        else:
                            partition_key = pickle.dumps(electron.key)

                    # Electrons are serialized with pickle
                    output = pickle.dumps(electron, protocol=4)

                    # If the destiny topic is not specified, the first is used
                    if not electron.topic:
                        if not self.output_topics:
                            util.print_error(self, "Electron / default output topic unset. Exiting...", fatal=True)

                        electron.topic = self.output_topics[0]

                    try:
                        # If partition_key = None, the partition.assignment.strategy
                        # is used to distribute the messages
                        self.producer.produce(topic=electron.topic,
                                              key=partition_key,
                                              value=output)

                        # Asynchronous
                        if self.asynchronous:
                            self.producer.poll(0)

                        # Synchronous
                        else:
                            # Wait for all messages in the Producer queue to be delivered.
                            self.producer.flush()

                    except Exception:
                        util.print_exception(self, "Kafka producer error. Exiting...", fatal=True)

            # Synchronous
            if self.synchronous:
                # If the item comes from the Kafka consumer, intended for
                # message commits
                if kafka_consumer_commit_callback:
                    self._execute_kafka_consumer_commit_callback(
                        kafka_consumer_commit_callback,
                        kafka_consumer_commit_callback_kwargs,
                        kafka_consumer_commit_callback_args)

                # Optional micromodule callback after processing ONE item
                # in the transform method. If multiple seperated electrons
                # are returned, multiple messages will be send but the callback
                # method will be called only once if all the messages were
                # sent correctly.
                if transform_callback:
                    if transform_callback_kwargs:
                        transform_callback(**transform_callback_kwargs)
                    elif transform_callback_args:
                        transform_callback(*transform_callback_args)
                    else:
                        transform_callback()

    def _break_consumer_loop(self):
        return len(subscription) > 1 and self.mki_mode != 'parity'

    def _kafka_consumer(self):
        # Since the list
        if not self.input_topics:
            logging.info(f"{self.__class__.__name__}: stopped input. No input topics.")
            return

        if self.mki_mode == 'parity':
            self.input_topic_assignments = {-1: -1}
        # If topics are not specified, the first is used
        elif not self.input_topic_assignments:
            self.input_topic_assignments = {}
            self.input_topic_assignments[self.input_topics[0]] = -1

        # Kafka Consumer
        properties = self.common_properties
        properties.update({
            'group.id': self.consumer_group,
            'session.timeout.ms': self.consumer_timeout,
            'default.topic.config': {
                'auto.offset.reset': 'smallest',
            }
        })

        # Asynchronous mode (default)
        if self.asynchronous:
            properties.update({
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000
            })

        consumer = Consumer(properties)
        logging.info(f'{self.__class__.__name__}: consumer group - {self.consumer_group}')
        running = True
        prev_queued_messages = 0
        while running:
            for i, topic in enumerate(self.input_topic_assignments.keys()):
                # Buffer for the current topic
                message_buffer = []

                if self.mki_mode == 'exp':
                    subscription = [topic]
                elif self.mki_mode == 'parity':
                    subscription = list(self.input_topics)
                else:
                    util.print_error(self, 'Unknown priority mode', fatal=True)

                # Replaces the current subscription
                consumer.subscribe(subscription)
                logging.info(f'{self.__class__.__name__} listening on: {subscription}')

                try:
                    start_time = util.get_current_timestamp()
                    assigned_time = self.input_topic_assignments[topic]
                    while assigned_time == -1 or Link.in_time(start_time, assigned_time):
                        # Subscribe to the topics again if input topics have changed
                        if self.changed_input_topics:
                            self.changed_input_topics = False
                            break

                        message = consumer.poll()

                        if not message.key() and not message.value():
                            if not self._break_consumer_loop:
                                continue
                            # New topic / restart if there are more topics or
                            # there aren't assigned partitions
                            break

                        if message.error():
                            # End of partition event, not really an error
                            if message.error().code() == \
                            KafkaError._PARTITION_EOF:
                                continue

                            elif message.error():
                                util.print_error(self, str(message.error()))
                        else:
                            # Synchronous commit
                            if self.synchronous:
                                # consumer.commit(asynchronous=False)
                                # Commit when the transformation is commited
                                self.queue.put((message, consumer.commit, {'message': message, 'asynchronous': False}))
                                continue

                            # Asynchronous (only one topic)
                            if len(subscription) == 1 or self.mki_mode == 'parity':
                                self.queue.put(message)
                                continue

                            # Asynchronous (with penalizations support for
                            # multiple topics)

                            # The message is added to a local list that will be
                            # dumped to a queue for asynchronous processing
                            message_buffer.append(message)
                            current_queued_messages = len(message_buffer)

                            self.queue.messages_left = \
                                self.queue.messages_left - 1

                            # If there is only one message left, the offset is
                            # committed
                            if self.queue.messages_left < 1:
                                for message in message_buffer:
                                    self.queue.put(message)
                                message_buffer = []

                                self.queue.messages_left = \
                                    self.queue.minimum_messages

                            # Penalize if only one message was consumed
                            if not self._break_consumer_loop \
                            and current_queued_messages > 1 \
                            and current_queued_messages > prev_queued_messages - 2:
                                logging.info(f"Penalized topic: {topic}")
                                break

                            prev_queued_messages = current_queued_messages

                    # Dump the buffer before changing the subscription
                    for message in message_buffer:
                        self.queue.put(message)

                except Exception as e:
                    util.print_exception(self, "Kafka consumer error. Exiting...")
                    try:
                        consumer.close()
                    except Exception:
                        util.print_exception(self, 'Exception when closing the consumer.', fatal=True)

        logging.info(f"{self.__class__.__name__}: stopped input.")

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

    def add_input_topic(self, input_topic):
        if input_topic not in self.input_topics:
            self.input_topics.append(input_topic)
            self.changed_input_topics = True
            if self.mki_mode == 'exp':
                self._set_input_topic_assignments()
            logging.info(self.__class__.__name__ + f' added input {input_topic}.')

    def remove_input_topic(self, input_topic):
        if input_topic in self.input_topics:
            self.input_topics.remove(input_topic)
            self.changed_input_topics = True
            if self.mki_mode == 'exp':
                self._set_input_topic_assignments()
            logging.info(self.__class__.__name__ + f' removed input: {input_topic}.')

    def start(self,
              link_mode=None,
              mki_mode='parity',
              consumer_group=None,
              asynchronous=True,
              synchronous=None,
              consumer_timeout=20000):
        self.link_mode = link_mode
        self.mki_mode = mki_mode
        if not consumer_group:
            self.consumer_group = self.__class__.__name__
        else:
            self.consumer_group = consumer_group

        self.asynchronous = asynchronous
        if synchronous:
            self.asynchronous = not synchronous
        self.synchronous = not asynchronous

        if self.asynchronous:
            logging.info(self.__class__.__name__ + ' execution mode: asynchronous.')
        else:
            logging.info(self.__class__.__name__ + ' execution mode: synchronous.')

        self.consumer_timeout = consumer_timeout

        self.queue = LinkQueue()

        self.common_properties = {
            'bootstrap.servers': self.kafka_host_port,
            'compression.codec': 'snappy',
            'api.version.request': True
        }

        try:
            self.aerospike = AerospikeConnector(
                self.aerospike_host,
                self.aerospike_port
            )
        except AttributeError:
            self.aerospike = None

        try:
            self.mongodb = MongodbConnector(
                self.mongodb_host,
                self.mongodb_port
            )
        except AttributeError:
            self.mongodb = None

        self.changed_input_topics = False

        # Overwritable by a link
        try:
            self.setup()
        except Exception:
            util.print_exception(self, "Exception during the execution of \"setup\". Exiting...", fatal=True)

        # Output
        output_kwargs, output_thread = self._get_output_opts()

        # Input
        input_kwargs, input_thread = self._get_input_thread()

        # Start threads
        self.threads = {'output': {'thread': output_thread,
                                   'kwargs': output_kwargs},
                        'input': {'thread': input_thread,
                                  'kwargs': input_kwargs}}
        output_thread.start()
        input_thread.start()
        logging.info(self.__class__.__name__ + ' link started.')

    def _get_output_opts(self):
        # Disable Kafka producer for custom output modes
        kafka_producer = True
        if self._is_custom_output():
            kafka_producer = False

        output_kwargs = {'target': self._output,
                         'kafka_producer': kafka_producer}
        output_thread = threading.Thread(target=Link._thread_target,
                                         kwargs=output_kwargs)
        return output_kwargs, output_thread

    def _get_input_thread(self):
        input_target = self._kafka_consumer  # Default Kafka input
        input_kwargs = {}

        # Custom input
        if self._is_custom_input():
            input_target = self.custom_input

        # Multiple Kafka input topics
        elif self._is_multiple_kafka_input():
            # Exponential window assignment
            if self.mki_mode == 'exp':
                self._set_input_topic_assignments()

        input_kwargs['target'] = input_target
        input_thread = threading.Thread(target=Link._thread_target,
                                        kwargs=input_kwargs)
        input_thread.daemon = True
        return input_kwargs, input_thread

    def _is_custom_output(self):
        return self.link_mode == Link.CUSTOM_OUTPUT \
        or self.link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT

    def _is_custom_input(self):
        return self.link_mode == Link.CUSTOM_INPUT

    def _is_kafka_input(self):
        return self._is_multiple_kafka_input() \
        or self.link_mode == Link.CUSTOM_OUTPUT

    def _is_kafka_output(self):
        return not self._is_custom_output()

    def _is_multiple_kafka_input(self):
        return self.link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT \
        or self.link_mode == Link.MULTIPLE_KAFKA_INPUTS

    def _set_input_topic_assignments(self):
        self.input_topic_assignments = {}
        window_size = 900  # in seconds, 15 minutes
        topics_no = len(self.input_topics)
        logging.info("Input topics time assingments:")
        for i, topic in enumerate(self.input_topics):
            topic_assingment = \
                self._get_index_assignment(window_size, i, topics_no)
            self.input_topic_assignments[topic] = topic_assingment
            logging.info(' - ' + topic + ": " + str(topic_assingment) +
                  " seconds")

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
                            '--input',
                            '--input-topic',
                            action="store",
                            dest="input_topics",
                            help='Kafka input topics. Several topics '
                            + 'can be specified separated by commas.',
                            required=False)
        # Output topic
        parser.add_argument('-o',
                            '--output',
                            '--output-topics',
                            action="store",
                            dest="output_topics",
                            help='Kafka output topics. Several topics '
                            + 'can be specified separated by commas.',
                            required=False)

        # Kafka bootstrap server
        parser.add_argument('-k',
                            '--kafka',
                            '-b',
                            '--kafka-bootstrap-server',
                            action="store",
                            dest="kafka_host_port",
                            help='Kafka bootstrap server. \
                            I.e., "localhost:9092".',
                            required=True)

        # Aerospike bootstrap server
        parser.add_argument('-a',
                            '--aerospike',
                            '--aerospike-bootstrap-server',
                            action="store",
                            dest="aerospike_host_port",
                            help='Aerospike bootstrap server. \
                            I.e., "localhost:3000".',
                            required=False)

        # MongoDB server
        parser.add_argument('-m',
                            '--mongodb',
                            action="store",
                            dest="mongodb_host_port",
                            help='MongoDB server. \
                            I.e., "localhost:27017".',
                            required=False)

        # Aerospike path
        parser.add_argument('-l',
                            '-p',
                            '--resources-location',
                            action="store",
                            dest="resources_location",
                            help='Path for setup resources. \
                            I.e., "aerospike:namespace:set".',
                            required=False)

        parsed_args = parser.parse_known_args()
        args = parsed_args[0]
        self.args = parsed_args[1]

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

        self.kafka_host_port = args.kafka_host_port

        if args.aerospike_host_port:
            aerospike_host_port = args.aerospike_host_port.split(':')
            self.aerospike_host = aerospike_host_port[0]
            self.aerospike_port = int(aerospike_host_port[1])

        if args.mongodb_host_port:
            mongodb_host_port = args.mongodb_host_port.split(':')
            self.mongodb_host = mongodb_host_port[0]
            self.mongodb_port = int(mongodb_host_port[1])

        if args.resources_location:
            resources_location = args.resources_location.split(':')
            self.resources_location = resources_location[0]

            if self.resources_location == 'aerospike':
                self.aerospike_resources_namespace = resources_location[1]
                self.aerospike_resources_set = resources_location[2]
            elif self.resources_location == 'local':
                self.local_resources_path = resources_location[1]
