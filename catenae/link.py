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
from . import utils
import argparse
from .connectors.aerospike import AerospikeConnector
from .connectors.mongodb import MongodbConnector
from .connectors.local import LocalConnector
import logging
from uuid import uuid4


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

    # Multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS = 0

    # Custom output without multiple kafka inputs
    CUSTOM_OUTPUT = 1

    # Custom input with multiple kafka inputs
    MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT = 2

    # Custom input
    CUSTOM_INPUT = 3

    def __init__(self, log_level='INFO'):
        logging.getLogger().setLevel(getattr(logging, log_level))
        self.launched = False
        self.input_topics_lock = threading.Lock()
        self.rpc_topic = f'catenae_rpc_{self.__class__.__name__.lower()}'
        self._load_args()

    def _loop_task(self, target, args, kwargs, interval):
        running = True
        while(running):
            try:
                logging.info(f"{self.__class__.__name__}: new loop iteration ({target.__name__})")
                start_timestamp = utils.get_timestamp()

                if args:
                    target(*args)
                elif kwargs:
                    target(**kwargs)
                else:
                    target()

                sleep_seconds = interval - utils.get_timestamp() + start_timestamp
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                    
            except Exception:
                logging.exception(f'Exception raised when executing the loop: {target.__name__}')

    def rpc_call(self, module, method, to=None, args=None, kwargs=None):
        """ 
        Send a Kafka message which will be interpreted as a RPC call by the receiver module.
        The to parameter referes to the consumer group which can be random (UUID), the name
        of the class (module) or a custom one
        """
        topic = f'catenae_rpc_{module.lower()}'
        electron = Electron(value={'method': method,
                                   'from': self.consumer_group,
                                   'args': args,
                                   'kwargs': kwargs},
                            topic=topic)
        if to:
            electron.value.update({'to': to})
        self.send(electron)

    def _rpc_call(self, method, from_=None, args=None, kwargs=None):
        """
        Local invocation of a RPC call coming from another module
        """
        logging.info(f'RPC invocation from {from_}')
        try:
            if args:
                getattr(self, method)(*args)
            elif kwargs:
                getattr(self, method)(**kwargs)
            else:
                getattr(self, method)()
        except Exception:
            logging.exception('Exception raised while executing a RPC call:')

    def loop(self, target, args=None, kwargs=None, interval=60, wait=False):
        loop_task_kwargs = {'target': target,
                            'args': args,
                            'kwargs': kwargs,
                            'interval': interval}
        loop_thread = threading.Thread(target=self._loop_task, kwargs=loop_task_kwargs)
        if wait:
            time.sleep(interval)
        loop_thread.start()
        
    def thread(self, target, args=None, kwargs=None):
        raise NotImplementedError

    def process(self, target, args=None, kwargs=None):
        raise NotImplementedError    

    def _execute_kafka_consumer_commit_callback(self,
        kafka_consumer_commit_callback,
        kafka_consumer_commit_callback_kwargs,
        kafka_consumer_commit_callback_args):

        kafka_consumer_commit_callback_done = False
        kafka_consumer_commit_callback_attempts = 0
        while not kafka_consumer_commit_callback_done:
            if kafka_consumer_commit_callback_attempts == 15:
                utils.print_error(self, 'Cannot commit a message. Exiting...', fatal=True)
            try:
                if kafka_consumer_commit_callback_kwargs:
                    kafka_consumer_commit_callback(**kafka_consumer_commit_callback_kwargs)
                elif kafka_consumer_commit_callback_args:
                    kafka_consumer_commit_callback(*kafka_consumer_commit_callback_args)
                else:
                    kafka_consumer_commit_callback()
                kafka_consumer_commit_callback_done = True
            except Exception as e:
                if 'UNKNOWN_MEMBER_ID' in str(e):
                    utils.print_error(self, 'Cannot commit a message (timeout). Exiting...', fatal=True)
                logging.exception(f'Trying to commit a message ({kafka_consumer_commit_callback_attempts})...')
                kafka_consumer_commit_callback_attempts += 1
                time.sleep(2)


    def _kakfa_producer(self):
        """
        Send to the Kafka broker electrons coming from the send method of
        resulting from the execution of the transform method
        """
        properties = dict(self.common_properties)
        properties.update({
            'partition.assignment.strategy': 'roundrobin',
            'message.max.bytes': 2097152, # 2MiB
            'socket.send.buffer.bytes': 0, # System default
            'request.required.acks': 1, # ACK from the leader
            'message.send.max.retries': 10,
            'queue.buffering.max.ms': 1,
            'max.in.flight.requests.per.connection': 1,
            'batch.num.messages': 1
        })

        if self.synchronous:
            properties.update({
                'message.send.max.retries': 10000000, # Max value
                'request.required.acks': -1, # ACKs from all replicas
                'max.in.flight.requests.per.connection': 1,
                'batch.num.messages': 1,
                # 'enable.idempotence': True, # not supported yet
            })

        # queue.buffering.max.ms - how long librdkafka will wait for
        # batch.num.messages to be produced by the application before
        # sending them to the broker in a produce request.

        # batch.num.messages - the maximum number of messages that will
        # go in a single produce request to the broker.

        # message.send.max.retries - the client resent the record upon 
        # receiving the error.

        # max.in.flight.requests.per.connection - if set to 1, only one
        # request can be sent to the broker at a time, guaranteeing the
        # order if retries is enabled.

        self.producer = Producer(properties)

        kafka_consumer_commit_callback = None
        kafka_consumer_commit_callback_args = None
        kafka_consumer_commit_callback_kwargs = None

        running = True
        while(running):
            electron = self._output_messages.get()

            # All the queue items of the _output_messages must be individual
            # instances of Electron
            if type(electron) != Electron:
                logging.info(type(electron))
                logging.info(str(electron))
                raise ValueError

            # The key is enconded for its use as partition key
            partition_key = None
            if electron.key:
                if type(electron.key) == str:
                    partition_key = electron.key.encode('utf-8')
                else:
                    partition_key = pickle.dumps(electron.key, protocol=4)

            # If the destiny topic is not specified, the first is used
            if not electron.topic:
                if not self.output_topics:
                    utils.print_error(self, "Electron / default output topic unset. Exiting...", fatal=True)
                electron.topic = self.output_topics[0]

            # Electrons are serialized
            if electron.unpack_if_string and type(electron.value) == str:
                serialized_electron = electron.value
            else:
                serialized_electron = pickle.dumps(electron, protocol=4)
                    
            try:
                # If partition_key = None, the partition.assignment.strategy
                # is used to distribute the messages
                self.producer.produce(topic=electron.topic,
                                        key=partition_key,
                                        value=serialized_electron)

                # Asynchronous
                if self.asynchronous:
                    self.producer.poll(0)

                # Synchronous
                else:
                    # Wait for all messages in the Producer queue to be delivered.
                    self.producer.flush()

            except Exception:
                utils.print_exception(self, "Kafka producer error. Exiting...", fatal=True)

            # Synchronous
            # If the item comes from the Kafka consumer, intended for
            # message commits
            if self.synchronous and kafka_consumer_commit_callback:
                self._execute_kafka_consumer_commit_callback(
                    kafka_consumer_commit_callback,
                    kafka_consumer_commit_callback_kwargs,
                    kafka_consumer_commit_callback_args)

    def _transform_handler(self, kafka_producer=True):
        """
        Process only messages coming from a Kafka input
        """
        running = True
        while running:

            transform_callback = None
            transform_callback_args = None
            transform_callback_kwargs = None

            kafka_consumer_commit_callback = None
            kafka_consumer_commit_callback_args = None
            kafka_consumer_commit_callback_kwargs = None

            queue_item = self._received_messages.get()

            # An item from the _received_messages queue will not be of 
            # type Electron in any case. In the nearest scenario, the 
            # Kafka message value would be an Electron instance

            # Tuple
            if type(queue_item) == tuple:
                kafka_consumer_commit_callback = queue_item[1]
                if len(queue_item) > 2:
                    if type(queue_item[2]) == list:
                        kafka_consumer_commit_callback_args = queue_item[2]
                    elif type(queue_item[2]) == dict:
                        kafka_consumer_commit_callback_kwargs = queue_item[2]
                queue_item = queue_item[0]

            # Electron instance
            if type(queue_item.value()) == Electron:
                electron = queue_item.value()

            # String or custom object
            elif type(queue_item.value()) == bytes:
                try:
                    # String
                    electron = Electron(queue_item.key(),
                                        queue_item.value().decode('utf-8'))
                except Exception:
                    # Other object
                    try:
                        electron = pickle.loads(queue_item.value())
                        if type(electron) != Electron:
                            electron = Electron(queue_item.key(), electron)
                    except Exception:
                        electron = Electron(queue_item.key(), queue_item.value())
            else:
                utils.print_error(self, 'Not supported type for ' + \
                f'{str(queue_item.value())} ({type(queue_item.value())})')
                continue

            # Clean the previous topic
            electron.previous_topic = queue_item.topic()
            electron.topic = None

            # The destiny topic will be overwritten if desired in the
            # transform method (default, first output topic)
            try:
                if electron.previous_topic == self.rpc_topic:
                    # The instance is not provided or matches the UUID that was
                    # used as consumer group
                    if not 'to' in electron.value or \
                    electron.value['to'] == self.consumer_group:
                        if 'method' in electron.value \
                        and ('args' in electron.value or 'kwargs' in electron.value):
                            self._rpc_call(electron.value['method'],
                                           from_=electron.value['from'],
                                           args=electron.value['args'],
                                           kwargs=electron.value['kwargs'])
                    else:
                        logging.error(f"Invalid RPC invocation: {electron.value}")
                    # Skip the standard procedure
                    continue

                else:
                    transform_result = self.transform(electron)
                    
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

                    # Already a list
                    if type(electrons) == list:
                        real_electrons = []
                        for electron in electrons:
                            if type(electron) == Electron:
                                real_electrons.append(electron)
                            else:
                                real_electrons.append(Electron(value=electron, unpack_if_string=True))
                        electrons = real_electrons

                    # If there is only one item, convert it to a list
                    else:
                        if type(electrons) == Electron:
                            electrons = [electrons]
                        else:
                            electrons = [Electron(value=electrons)]

                    # If the transform method returns anything and the output
                    # is set to the Kafka producer, delegate the remaining
                    # work to the Kafka producer

                    if electrons and kafka_producer:
                        for electron in electrons:
                            self._output_messages.put(electron)
                        continue

                    # If the synchronous mode is enabled, the input message
                    # will be only commited if the returned by the transform
                    # method is None or if the output is not managed by the 
                    # Kafka producer

                    # Synchronous
                    if self.synchronous:
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
                                
                        # If the item comes from the Kafka consumer, intended for
                        # message commits
                        if kafka_consumer_commit_callback:
                            self._execute_kafka_consumer_commit_callback(
                                kafka_consumer_commit_callback,
                                kafka_consumer_commit_callback_kwargs,
                                kafka_consumer_commit_callback_args)

            except Exception:
                utils.print_exception(
                    self,
                    'Exception during the execution of "transform"')

    def _break_consumer_loop(self, subscription):
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
        properties = dict(self.common_properties)
        properties.update({
            'max.partition.fetch.bytes': 2097152, # 2MiB,
            'metadata.max.age.ms': 10000,
            'socket.receive.buffer.bytes': 0, # System default
            'group.id': self.consumer_group,
            'session.timeout.ms': self.consumer_timeout,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })

        # Synchronous mode
        if self.synchronous:
            properties.update({
                'enable.auto.commit': False,
                'auto.commit.interval.ms': 0
            })

        consumer = Consumer(properties)
        logging.info(f'{self.__class__.__name__} consumer group: {self.consumer_group}')
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
                    utils.print_error(self, 'Unknown priority mode', fatal=True)

                # Replaces the current subscription
                consumer.subscribe(subscription)
                logging.info(f'{self.__class__.__name__} listening on: {subscription}')

                try:
                    start_time = utils.get_timestamp_ms()
                    assigned_time = self.input_topic_assignments[topic]
                    while assigned_time == -1 or Link.in_time(start_time, assigned_time):
                        # Subscribe to the topics again if input topics have changed
                        self.input_topics_lock.acquire()
                        if self.changed_input_topics:
                            self.changed_input_topics = False
                            self.input_topics_lock.release()
                            break
                        self.input_topics_lock.release()

                        message = consumer.poll()

                        if not message or (not message.key() and not message.value()):
                            if not self._break_consumer_loop(subscription):
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
                                utils.print_error(self, str(message.error()))
                        else:
                            # Synchronous commit
                            if self.synchronous:
                                # Commit when the transformation is commited
                                self._received_messages.put((message, consumer.commit, {'message': message, 'asynchronous': False}))
                                continue

                            # Asynchronous (only one topic)
                            if len(subscription) == 1 or self.mki_mode == 'parity':
                                self._received_messages.put(message)
                                continue

                            # Asynchronous (with penalizations support for
                            # multiple topics)

                            # The message is added to a local list that will be
                            # dumped to a queue for asynchronous processing
                            message_buffer.append(message)
                            current_queued_messages = len(message_buffer)

                            self._received_messages.messages_left = \
                                self._received_messages.messages_left - 1

                            # If there is only one message left, the offset is
                            # committed
                            if self._received_messages.messages_left < 1:
                                for message in message_buffer:
                                    self._received_messages.put(message)
                                message_buffer = []

                                self._received_messages.messages_left = \
                                    self._received_messages.minimum_messages

                            # Penalize if only one message was consumed
                            if not self._break_consumer_loop(subscription) \
                            and current_queued_messages > 1 \
                            and current_queued_messages > prev_queued_messages - 2:
                                logging.info(f"Penalized topic: {topic}")
                                break

                            prev_queued_messages = current_queued_messages

                    # Dump the buffer before changing the subscription
                    for message in message_buffer:
                        self._received_messages.put(message)

                except Exception:
                    utils.print_exception(self, "Kafka consumer error. Exiting...")
                    try:
                        consumer.close()
                    except Exception:
                        utils.print_exception(self, 'Exception when closing the consumer.', fatal=True)

        logging.info(f"{self.__class__.__name__}: stopped input.")

    def _get_index_assignment(self, window_size, index, elements_no, base=1.7):
        """
        window_size implies a full cycle consuming all the queues with
        priority.
        """
        aggregated_value = .0

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

    def send(self, output_content, topic=None):
        try:
            if type(output_content) == Electron:
                if topic:
                    output_content.topic = topic
                output_content = output_content.copy()
                self._output_messages.put(output_content)
                return
            if type(output_content != list):
                self._output_messages.put(Electron(value=output_content, topic=topic, unpack_if_string=True))
                return
            if type(output_content) == list:
                for item in output_content:
                    if type(item) == Electron:
                        self._output_messages.put(item)
                        continue
                    self._output_messages.put(Electron(value=item, topic=topic, unpack_if_string=True))
        except Exception:
            logging.exception('')

    def generator(self):
        """ If the generator method was not overrided in the main script an
        error will be printed and the execution will finish """
        utils.print_error(self,
                         "Undefined \"generator\" method. Exiting...",
                         fatal=True)

    def custom_input(self):
        """ If a custom_input method is not defined by the main script,
        the new standard generator will be invoked. This is to support both
        method names for previous modules, but custom_input should be
        considered deprecated """
        return self.generator()

    @staticmethod
    def _thread_target(**kwargs):
        try:
            target = kwargs['target']
            kwargs.pop('target')
            target(**kwargs)
        except Exception:
            utils.print_exception(target, f"Exception during the execution of \"{target.__name__}\". Exiting...", fatal=True)

    def add_input_topic(self, input_topic):
        if input_topic not in self.input_topics:
            self.input_topics.append(input_topic)
            self.input_topics_lock.acquire()
            if self.mki_mode == 'exp':
                self._set_input_topic_exp_assignments()
            self.changed_input_topics = True
            self.input_topics_lock.release()
            logging.info(self.__class__.__name__ + f' added input {input_topic}.')

    def remove_input_topic(self, input_topic):
        if input_topic in self.input_topics:
            self.input_topics.remove(input_topic)
            self.input_topics_lock.acquire()
            if self.mki_mode == 'exp':
                self._set_input_topic_exp_assignments()
            self.changed_input_topics = True
            self.input_topics_lock.release()
            logging.info(self.__class__.__name__ + f' removed input: {input_topic}.')

    def start(self,
              link_mode=None,
              mki_mode='parity',
              consumer_group=None,
              asynchronous=True,
              synchronous=None,
              consumer_timeout=20000,
              random_consumer_group=False):

        if self.launched:
            return
        self.launched = True

        self.link_mode = link_mode
        self.mki_mode = mki_mode

        if not hasattr(self, 'consumer_group'):
            if random_consumer_group:
                self.consumer_group = str(uuid4())
            elif consumer_group:
                self.consumer_group = consumer_group
            else:
                self.consumer_group = self.__class__.__name__

        self.asynchronous = asynchronous
        if synchronous:
            self.asynchronous = not synchronous
        self.synchronous = not self.asynchronous

        if self.asynchronous:
            logging.info(self.__class__.__name__ + ' execution mode: asynchronous.')
        else:
            logging.info(self.__class__.__name__ + ' execution mode: synchronous.')

        self.consumer_timeout = consumer_timeout

        self._received_messages = LinkQueue()
        self._output_messages = LinkQueue()

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
            utils.print_exception(self, "Exception during the execution of \"setup\". Exiting...", fatal=True)

        # Transform
        # Disable Kafka producer for custom output modes
        kafka_producer = True
        if self._is_custom_output():
            kafka_producer = False
        transform_kwargs = {'target': self._transform_handler,
                            'kafka_producer': kafka_producer}
        transform_thread = threading.Thread(target=Link._thread_target,
                                            kwargs=transform_kwargs)
        transform_thread.start()

        # Kafka producer
        if kafka_producer:
            producer_kwargs = {'target': self._kakfa_producer}
            producer_thread = threading.Thread(target=Link._thread_target,
                                            kwargs=producer_kwargs)
            producer_thread.start()

        # Kafka consumer
        input_target = self._kafka_consumer
        if self._is_custom_input():
            input_target = self.custom_input
        elif self._is_multiple_kafka_input() and self.mki_mode == 'exp':
            self._set_input_topic_exp_assignments()
        consumer_kwargs = {'target': input_target}
        consumer_thread = threading.Thread(target=Link._thread_target,
                                           kwargs=consumer_kwargs)
        consumer_thread.start()

        logging.info(self.__class__.__name__ + ' link started.')

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

    def _set_input_topic_exp_assignments(self):
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
        return (start_time - utils.get_timestamp_ms()) < assigned_time

    def load_object(self, object_name):
        try:
            if self.resources_location == 'aerospike':
                _, obj = self.aerospike.get_and_close(
                    object_name,
                    self.aerospike_resources_namespace,
                    self.aerospike_resources_set
                )
                return obj
            elif self.resources_location == 'local':
                local_connector = LocalConnector(self.local_resources_path)
                return local_connector.get_object(object_name)
        except Exception:
            utils.print_exception(self,
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

        # Kafka consumer group
        parser.add_argument('-g',
                            '--consumer-group',
                            action="store",
                            dest="consumer_group",
                            help='Kafka consumer group. \
                            E.g., "filter_group".',
                            required=False)

        # Kafka bootstrap server
        parser.add_argument('-k',
                            '--kafka',
                            '-b',
                            '--kafka-bootstrap-server',
                            action="store",
                            dest="kafka_host_port",
                            help='Kafka bootstrap server. \
                            E.g., "localhost:9092".',
                            required=True)

        # Aerospike bootstrap server
        parser.add_argument('-a',
                            '--aerospike',
                            '--aerospike-bootstrap-server',
                            action="store",
                            dest="aerospike_host_port",
                            help='Aerospike bootstrap server. \
                            E.g., "localhost:3000".',
                            required=False)

        # MongoDB server
        parser.add_argument('-m',
                            '--mongodb',
                            action="store",
                            dest="mongodb_host_port",
                            help='MongoDB server. \
                            E.g., "localhost:27017".',
                            required=False)

        # Aerospike path
        parser.add_argument('-l',
                            '-p',
                            '--resources-location',
                            action="store",
                            dest="resources_location",
                            help='Path for setup resources. \
                            E.g., "aerospike:namespace:set".',
                            required=False)

        parsed_args = parser.parse_known_args()
        args = parsed_args[0]
        self.args = parsed_args[1]

        # If no commas, a list with 1 element is returned
        # Input topics
        if args.input_topics:
            self.input_topics = args.input_topics.split(',')
        else:
            self.input_topics = []

        # Add the default topic for RPC invocations
        self.input_topics.append(self.rpc_topic)

        # Output topics
        if args.output_topics:
            self.output_topics = args.output_topics.split(',')
        else:
            self.output_topics = []

        # Consumer group
        if args.consumer_group:
            self.consumer_group = args.consumer_group

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
