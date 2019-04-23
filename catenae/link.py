#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
from threading import Lock
from pickle5 import pickle
import time
import argparse
from uuid import uuid4
import os
from confluent_kafka import Producer, Consumer, KafkaError
from . import utils
from .electron import Electron
from .callback import Callback
from .logger import Logger
from .custom_queue import LinkQueue
from .custom_threading import Thread, ThreadPool
from .connectors.aerospike import AerospikeConnector
from .connectors.mongodb import MongodbConnector


class Link:
    MULTIPLE_KAFKA_INPUTS = 0
    CUSTOM_OUTPUT = 1
    MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT = 2
    CUSTOM_INPUT = 3

    def __init__(self,
                 log_level='INFO',
                 link_mode=None,
                 input_mode='parity',
                 consumer_group=None,
                 consumer_timeout=20,
                 random_consumer_group=False,
                 synchronous=False,
                 sequential=False,
                 num_rpc_threads=5,
                 num_main_threads=5):

        self._set_log_level(log_level)
        self.logger = Logger(self, self._log_level)
        self.logger.log(f'log level: {self._log_level}')

        self._launched = False
        self._input_topics_lock = Lock()

        # Preserve the id if the container restarts
        if 'CATENAE_DOCKER' in os.environ \
        and bool(os.environ['CATENAE_DOCKER']):
            self._uid = os.environ['HOSTNAME']
        else:
            self._uid = utils.keccak256(str(uuid4()))[:12]

        # RPC topics
        self.rpc_instance_topic = f'catenae_rpc_{self._uid}'
        self.rpc_group_topic = f'catenae_rpc_{self.__class__.__name__.lower()}'
        self.rpc_broadcast_topic = 'catenae_rpc_broadcast'
        self._rpc_topics = [self.rpc_instance_topic, self.rpc_group_topic, self.rpc_broadcast_topic]

        self._load_args()

        self._set_link_mode_and_booleans(link_mode)
        self._set_consumer_group(consumer_group, random_consumer_group)
        self._set_execution_opts(synchronous, sequential, num_rpc_threads, num_main_threads, input_mode)
        self._set_consumer_timeout(consumer_timeout)

        self._input_messages = LinkQueue()
        self._output_messages = LinkQueue()
        self._changed_input_topics = False

    @property
    def input_topics(self):
        return self._input_topics

    @property
    def output_topics(self):
        return self._output_topics

    @property
    def consumer_group(self):
        return self._consumer_group

    @property
    def args(self):
        return self._args

    @property
    def uid(self):
        return self._uid

    @property
    def aerospike(self):
        return self._aerospike

    @property
    def mongodb(self):
        return self._mongodb

    def _loop_task(self, thread, target, args=None, kwargs=None, interval=None, wait=False):
        if wait:
            time.sleep(interval)

        while not thread.stopped():
            try:
                self.logger.log(f'new loop iteration ({target.__name__})')
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
                self.logger.log(f'exception raised when executing the loop: {target.__name__}', level='exception')

    def rpc_call(self, to='broadcast', method=None, args=None, kwargs=None):
        """ 
        Send a Kafka message which will be interpreted as a RPC call by the receiver module.
        """
        if not method:
            raise ValueError
        topic = f'catenae_rpc_{to.lower()}'
        electron = Electron(value={
            'method': method,
            'context': {
                'group': self._consumer_group,
                'uid': self._uid
            },
            'args': args,
            'kwargs': kwargs
        },
                            topic=topic)
        self.send(electron)

    def _rpc_call(self, electron, commit_kafka_message_callback):
        if not 'method' in electron.value:
            self.logger.log(f'invalid RPC invocation: {electron.value}', level='error')
            return

        try:
            context = electron.value['context']
            self.logger.log(
                f"RPC invocation from {electron.value['context']['uid']} ({electron.value['context']['group']})",
                level='debug')
            if electron.value['kwargs']:
                kwargs = electron.value['kwargs']
                kwargs.update({'context': electron.value['kwargs']})
                getattr(self, electron.value['method'])(**kwargs)
            else:
                args = electron.value['args']
                if not args:
                    args = [context]
                elif type(args) != list:
                    args = [context, args]
                else:
                    args = [context] + args
                getattr(self, electron.value['method'])(*args)

        except Exception:
            self.logger.log('error when invoking a RPC method', level='exception')
        finally:
            if self._synchronous:
                commit_kafka_message_callback.execute()
            return

    def suicide(self, message=None, exception=False, exit_code=1):
        if not message:
            message = 'Suicide method invoked'

        if self._kafka_endpoint:
            if hasattr(self, 'producer_thread'):
                self._producer_thread.stop()
            if hasattr(self, 'consumer_rpc_thread'):
                self._consumer_rpc_thread.stop()
            if hasattr(self, 'consumer_main_thread'):
                self._consumer_main_thread.stop()
            if hasattr(self, 'transform_thread'):
                self._transform_thread.stop()

        message += ' Exiting...'
        if exception:
            self.logger.log(message, level='exception')
            exit_code = 1
        elif exit_code == 0:
            self.logger.log(message)
        else:
            self.logger.log(message, level='error')

        os._exit(exit_code)

    def loop(self, target, args=None, kwargs=None, interval=60, wait=False):
        loop_task_kwargs = {'target': target, 'args': args, 'kwargs': kwargs, 'interval': interval, 'wait': wait}
        loop_thread = Thread(target=self._loop_task, kwargs=loop_task_kwargs)
        loop_task_kwargs.update({'thread': loop_thread})
        loop_thread.start()
        return loop_thread

    def thread(self, target, args=None, kwargs=None):
        raise NotImplementedError

    def process(self, target, args=None, kwargs=None):
        raise NotImplementedError

    def _kafka_producer(self):
        if self._synchronous:
            properties = dict(self._kafka_producer_synchronous_properties)
        else:
            properties = dict(self._kafka_producer_common_properties)

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

        self._producer = Producer(properties)
        self.logger.log(f'Producer properties: {utils.dump_dict_pretty(properties)}')

        while not self._producer_thread.stopped():
            electron = self._output_messages.get()

            # All the queue items of the _output_messages must be individual
            # instances of Electron
            if type(electron) != Electron:
                raise ValueError

            # The key is enconded for its use as partition key
            partition_key = None
            if electron.key:
                if type(electron.key) == str:
                    partition_key = electron.key.encode('utf-8')
                else:
                    partition_key = pickle.dumps(electron.key, protocol=pickle.HIGHEST_PROTOCOL)
            # Same partition key for the current instance if sequential mode
            # is enabled so consumer can get messages in order
            elif self._sequential:
                partition_key = self._uid.encode('utf-8')

            # If the destiny topic is not specified, the first is used
            if not electron.topic:
                if not self._output_topics:
                    self.suicide('Electron / default output topic unset')
                electron.topic = self._output_topics[0]

            # Electrons are serialized
            if electron.unpack_if_string and type(electron.value) == str:
                serialized_electron = electron.value
            else:
                serialized_electron = pickle.dumps(electron.get_sendable(), protocol=pickle.HIGHEST_PROTOCOL)

            try:
                # If partition_key = None, the partition.assignment.strategy
                # is used to distribute the messages
                self._producer.produce(topic=electron.topic, key=partition_key, value=serialized_electron)

                # Asynchronous
                if self._asynchronous:
                    self._producer.poll(5)

                # Synchronous
                else:
                    # Wait for all messages in the Producer queue to be delivered.
                    self._producer.flush()

                self.logger.log('electron produced', level='debug')

            except Exception:
                self.suicide('Kafka producer error', exception=True)

            # Synchronous
            if self._synchronous:
                for callback in electron.callbacks:
                    callback.execute()

    def _transform(self, electron, commit_kafka_message_callback, transform_callback):
        try:
            transform_result = self.transform(electron)
            self.logger.log('electron transformed', level='debug')
        except Exception:
            self.logger.log('exception during the execution of "transform"', level='exception')
            return

        if type(transform_result) == tuple:
            electrons = transform_result[0]
            # Function to call if asynchronous mode is enabled after
            # a message is correctly commited to a Kafka broker
            if len(transform_result) > 1:
                transform_callback.target = transform_result[1]
                if len(transform_result) > 2:
                    if type(transform_result[2]) == list:
                        transform_callback.args = transform_result[2]
                    elif type(transform_result[2]) == dict:
                        transform_callback.kwargs = transform_result[2]
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

        if electrons and self._is_kafka_output:
            # The callback will be executed only for the last
            # electron if there are more than one
            electrons[-1].callbacks = []
            if commit_kafka_message_callback:
                electrons[-1].callbacks.append(commit_kafka_message_callback)
            if transform_callback:
                electrons[-1].callbacks.append(transform_callback)

            for electron in electrons:
                self._output_messages.put(electron)
            return

        # If the synchronous mode is enabled, the input message
        # will be commited if the transform method returns None
        # or if the output is not managed by the Kafka producer
        if self._synchronous and commit_kafka_message_callback:
            commit_kafka_message_callback.execute()

    def _input_handler(self):
        while not self._transform_thread.stopped():
            self.logger.log('waiting for a new electron to transform...', level='debug')

            transform_callback = Callback()
            commit_kafka_message_callback = Callback(type_=Callback.COMMIT_KAFKA_MESSAGE)

            queue_item = self._input_messages.get()
            self.logger.log('electron received', level='debug')

            # An item from the _input_messages queue will not be of
            # type Electron in any case. In the nearest scenario, the
            # Kafka message value would be an Electron instance

            # Tuple
            if type(queue_item) == tuple:
                commit_kafka_message_callback.target = queue_item[1]
                if len(queue_item) > 2:
                    if type(queue_item[2]) == list:
                        commit_kafka_message_callback.args = queue_item[2]
                    elif type(queue_item[2]) == dict:
                        commit_kafka_message_callback.kwargs = queue_item[2]
                queue_item = queue_item[0]

            # Electron instance
            if type(queue_item.value()) == Electron:
                electron = queue_item.value()

            # String or custom object
            elif type(queue_item.value()) == bytes:
                try:
                    # String
                    electron = Electron(queue_item.key(), queue_item.value().decode('utf-8'))
                except Exception:
                    # Other object
                    try:
                        electron = pickle.loads(queue_item.value())
                        if type(electron) != Electron:
                            electron = Electron(queue_item.key(), electron)
                    except Exception:
                        electron = Electron(queue_item.key(), queue_item.value())
            else:
                self.logger.log('Not supported type for ' + \
                f'{str(queue_item.value())} ({type(queue_item.value())})', level='error')
                continue

            # Clean the previous topic
            electron.previous_topic = queue_item.topic()
            electron.topic = None

            # The destiny topic will be overwritten if desired in the
            # transform method (default, first output topic)
            if electron.previous_topic in self._rpc_topics:
                self._transform_rpc_executor.submit(self._rpc_call, [electron, commit_kafka_message_callback])
            else:
                self._transform_main_executor.submit(self._transform,
                                                     [electron, commit_kafka_message_callback, transform_callback])

    def _break_consumer_loop(self, subscription):
        return len(subscription) > 1 and self._input_mode != 'parity'

    def _commit_kafka_message(self, consumer, message):
        commited = False
        attempts = 0
        self.logger.log(f'trying to commit the message with value {message.value()} (attempt {attempts})',
                        level='debug')
        while not commited:
            if attempts > 1:
                self.logger.log(f'trying to commit the message with value {message.value()} (attempt {attempts})',
                                level='warn')
            try:
                consumer.commit(**{'message': message, 'asynchronous': False})
            except Exception:
                self.logger.log(f'exception when trying to commit the message with value {message.value()}',
                                level='exception')
                continue
            commited = True
            attempts += 1
        self.logger.log(f'Message with value {message.value()} commited', level='debug')

    def _kafka_consumer_rpc(self):
        properties = dict(self._kafka_consumer_synchronous_properties)
        properties.update({'group.id': self._uid})
        consumer = Consumer(properties)
        self.logger.log(f'[RPC] consumer properties: {utils.dump_dict_pretty(properties)}')
        subscription = list(self._rpc_topics)
        consumer.subscribe(subscription)
        self.logger.log(f'[RPC] listening on: {subscription}')

        while not self._consumer_rpc_thread.stopped():
            try:
                message = consumer.poll()

                if not message or (not message.key() and not message.value()):
                    if not self._break_consumer_loop(subscription):
                        continue
                    # New topic / restart if there are more topics or
                    # there aren't assigned partitions
                    break

                if message.error():
                    # End of partition is not an error
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        self.logger.log(str(message.error()), level='error')
                    continue

                # Commit when the transformation is commited
                self._input_messages.put((message, self._commit_kafka_message, [consumer, message]))

            except Exception:
                try:
                    consumer.close()
                finally:
                    self.logger.log('stopped RPC input')
                    self.suicide('Kafka consumer error', exception=True)

    def _kafka_consumer_main(self):
        # Since the list
        while not self._input_topics:
            self.logger.log('No input topics, waiting...', level='debug')
            time.sleep(1)

        if self._input_mode == 'parity':
            self._input_topic_assignments = {-1: -1}

        # If topics are not specified, the first is used
        elif not self._input_topic_assignments:
            self._input_topic_assignments = {}
            self._input_topic_assignments[self._input_topics[0]] = -1

        # Kafka Consumer
        if self._synchronous:
            properties = dict(self._kafka_consumer_synchronous_properties)
        else:
            properties = dict(self._kafka_consumer_common_properties)

        consumer = Consumer(properties)
        self.logger.log(f'[MAIN] consumer properties: {utils.dump_dict_pretty(properties)}')
        prev_queued_messages = 0

        while not self._consumer_main_thread.stopped():
            for topic in self._input_topic_assignments.keys():
                # Buffer for the current topic
                message_buffer = []

                if self._input_mode == 'exp':
                    subscription = [topic]
                elif self._input_mode == 'parity':
                    subscription = list(self._input_topics)
                else:
                    self.suicide('Unknown priority mode')

                # Replaces the current subscription
                consumer.subscribe(subscription)
                self.logger.log(f'[MAIN] listening on: {subscription}')

                try:
                    start_time = utils.get_timestamp_ms()
                    assigned_time = self._input_topic_assignments[topic]
                    while assigned_time == -1 or Link.in_time(start_time, assigned_time):
                        # Subscribe to the topics again if input topics have changed
                        self._input_topics_lock.acquire()
                        if self._changed_input_topics:
                            self._changed_input_topics = False
                            self._input_topics_lock.release()
                            break
                        self._input_topics_lock.release()

                        message = consumer.poll()

                        if not message or (not message.key() and not message.value()):
                            if not self._break_consumer_loop(subscription):
                                continue
                            # New topic / restart if there are more topics or
                            # there aren't assigned partitions
                            break

                        if message.error():
                            # End of partition is not an error
                            if message.error().code() != KafkaError._PARTITION_EOF:
                                self.logger.log(str(message.error()), level='error')
                            continue

                        else:
                            # Synchronous commit
                            if self._synchronous:
                                # Commit when the transformation is commited
                                self._input_messages.put((message, self._commit_kafka_message, [consumer, message]))
                                continue

                            # Asynchronous (only one topic)
                            if len(subscription) == 1 or self._input_mode == 'parity':
                                self._input_messages.put(message)
                                continue

                            # Asynchronous (with penalizations support for
                            # multiple topics)

                            # The message is added to a local list that will be
                            # dumped to a queue for asynchronous processing
                            message_buffer.append(message)
                            current_queued_messages = len(message_buffer)

                            self._input_messages.messages_left = \
                                self._input_messages.messages_left - 1

                            # If there is only one message left, the offset is
                            # committed
                            if self._input_messages.messages_left < 1:
                                for message in message_buffer:
                                    self._input_messages.put(message)
                                message_buffer = []

                                self._input_messages.messages_left = \
                                    self._input_messages.minimum_messages

                            # Penalize if only one message was consumed
                            if not self._break_consumer_loop(subscription) \
                            and current_queued_messages > 1 \
                            and current_queued_messages > prev_queued_messages - 2:
                                self.logger.log(f'penalized topic: {topic}')
                                break

                            prev_queued_messages = current_queued_messages

                    # Dump the buffer before changing the subscription
                    for message in message_buffer:
                        self._input_messages.put(message)

                except Exception:
                    try:
                        consumer.close()
                    finally:
                        self.logger.log('stopped main input')
                        self.suicide('Kafka consumer error', exception=True)

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

    def transform(self, _):
        # If the transform method was left blank, stop the main consumer
        if hasattr(self, 'consumer_main_thread'):
            self._consumer_main_thread.stop()

    def send(self, output_content, topic=None):
        try:
            if type(output_content) == Electron:
                if topic:
                    output_content.topic = topic
                output_content = output_content.deepcopy()
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
            self.logger.log(level='exception')

    def generator(self):
        """ If the generator method was not overrided in the main script an
        error will be printed and the execution will finish """
        self.suicide('Undefined "generator" method')

    def custom_input(self):
        """ If a custom_input method is not defined by the main script,
        the new standard generator will be invoked. This is to support both
        method names for previous modules, but custom_input should be
        considered deprecated """
        return self.generator()

    def _thread_target(self, target, args=None, kwargs=None):
        try:
            if args:
                target(*args)
            elif kwargs:
                target(**kwargs)
            else:
                target()
        except Exception:
            self.suicide(f'Exception during the execution of "{target.__name__}"', exception=True)

    def add_input_topic(self, input_topic):
        if input_topic not in self._input_topics:
            self._input_topics.append(input_topic)
            self._input_topics_lock.acquire()
            if self._input_mode == 'exp':
                self._set_input_topic_exp_assignments()
            self._changed_input_topics = True
            self._input_topics_lock.release()
            self.logger.log(f'added input {input_topic}')

    def remove_input_topic(self, input_topic):
        if input_topic in self._input_topics:
            self._input_topics.remove(input_topic)
            self._input_topics_lock.acquire()
            if self._input_mode == 'exp':
                self._set_input_topic_exp_assignments()
            self._changed_input_topics = True
            self._input_topics_lock.release()
            self.logger.log(f'removed input {input_topic}')

    def start(self):
        if self._launched:
            return
        self._launched = True

        self._set_kafka_common_properties()
        self._set_connectors()

        # Overwritable by a link
        try:
            self.setup()
        except Exception:
            self.suicide('Exception during the execution of "setup"', exception=True)

        self._launch_threads()
        self.logger.log(f'link {self._uid} is running')
        self.logger.log(f'consumer group: {self._consumer_group}')

    def _launch_threads(self):
        if not self._kafka_endpoint:
            self.logger.log('Kafka disabled')
            return

        # Transform
        self._transform_rpc_executor = ThreadPool(self, self._num_rpc_threads)
        self._transform_main_executor = ThreadPool(self, self._num_main_threads)

        transform_kwargs = {'target': self._input_handler}
        self._transform_thread = Thread(target=self._thread_target, kwargs=transform_kwargs)
        self._transform_thread.start()

        # Kafka producer
        if self._is_kafka_output:
            producer_kwargs = {'target': self._kafka_producer}
            self._producer_thread = Thread(target=self._thread_target, kwargs=producer_kwargs)
            self._producer_thread.start()

        # Kafka RPC consumer
        consumer_kwargs = {'target': self._kafka_consumer_rpc}
        self._consumer_rpc_thread = Thread(target=self._thread_target, kwargs=consumer_kwargs)
        self._consumer_rpc_thread.start()

        # Kafka main consumer
        input_target = self._kafka_consumer_main
        if self._is_custom_input:
            input_target = self.custom_input
        elif self._is_multiple_kafka_input and self._input_mode == 'exp':
            self._set_input_topic_exp_assignments()
        consumer_kwargs = {'target': input_target}
        self._consumer_main_thread = Thread(target=self._thread_target, kwargs=consumer_kwargs)
        self._consumer_main_thread.start()

    def _set_log_level(self, log_level):
        if not hasattr(self, '_log_level'):
            self._log_level = log_level.upper()

    def _set_connectors(self):
        try:
            self._aerospike = AerospikeConnector(self._aerospike_host, self._aerospike_port)
        except AttributeError:
            self._aerospike = None

        try:
            self._mongodb = MongodbConnector(self._mongodb_host, self._mongodb_port)
        except AttributeError:
            self._mongodb = None

    def _set_consumer_group(self, consumer_group, random_consumer_group):
        if hasattr(self, 'consumer_group'):
            consumer_group = self._consumer_group
        if hasattr(self, 'random_consumer_group'):
            random_consumer_group = self._random_consumer_group
        if random_consumer_group:
            self._consumer_group = self._uid
        elif consumer_group:
            self._consumer_group = consumer_group
        else:
            self._consumer_group = self.__class__.__name__

    def _set_link_mode_and_booleans(self, link_mode):
        if not hasattr(self, 'link_mode'):
            self._link_mode = link_mode
        self._is_custom_output = self._link_mode == Link.CUSTOM_OUTPUT \
            or self._link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT
        self._is_kafka_output = not self._is_custom_output
        self._is_custom_input = self._link_mode == Link.CUSTOM_INPUT
        self._is_multiple_kafka_input = \
            self._link_mode == Link.MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT \
                or self._link_mode == Link.MULTIPLE_KAFKA_INPUTS
        self._is_kafka_input = self._is_multiple_kafka_input \
            or self._link_mode == Link.CUSTOM_OUTPUT

    def _set_kafka_common_properties(self):
        common_properties = {
            'bootstrap.servers': self._kafka_endpoint,
            'compression.codec': 'snappy',
            'api.version.request': True
        }

        self._kafka_consumer_common_properties = dict(common_properties)
        self._kafka_consumer_common_properties.update({
            'max.partition.fetch.bytes': 1048576,  # 1MiB,
            'metadata.max.age.ms': 10000,
            'socket.receive.buffer.bytes': 0,  # System default
            'group.id': self._consumer_group,
            'session.timeout.ms': self._consumer_timeout,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })

        self._kafka_consumer_synchronous_properties = dict(self._kafka_consumer_common_properties)
        self._kafka_consumer_synchronous_properties.update({'enable.auto.commit': False, 'auto.commit.interval.ms': 0})

        self._kafka_producer_common_properties = dict(common_properties)
        self._kafka_producer_common_properties.update({
            'partition.assignment.strategy': 'roundrobin',
            'message.max.bytes': 1048576,  # 1MiB
            'socket.send.buffer.bytes': 0,  # System default
            'request.required.acks': 1,  # ACK from the leader
            # 'message.timeout.ms': 0, # (delivery.timeout.ms) Time a produced message waits for successful delivery
            # 'request.timeout.ms': 30000,
            'message.send.max.retries': 10,
            'queue.buffering.max.ms': 1,
            'max.in.flight.requests.per.connection': 1,
            'batch.num.messages': 1
        })

        self._kafka_producer_synchronous_properties = dict(self._kafka_producer_common_properties)
        self._kafka_producer_synchronous_properties.update({
            'message.send.max.retries': 10000000,  # Max value
            'request.required.acks': -1,  # ACKs from all replicas
            'max.in.flight.requests.per.connection': 1,
            'batch.num.messages': 1,
            # 'enable.idempotence': True, # not supported yet
        })

    def _set_execution_opts(self, synchronous, sequential, num_rpc_threads, num_main_threads, input_mode):
        if hasattr(self, 'sequential'):
            sequential = self._sequential
        if hasattr(self, 'synchronous'):
            synchronous = self._synchronous
        elif hasattr(self, 'asynchronous'):
            synchronous = not self._asynchronous
        if hasattr(self, 'num_rpc_threads'):
            num_rpc_threads = self._num_rpc_threads
        if hasattr(self, 'num_main_threads'):
            num_main_threads = self._num_main_threads

        if synchronous:
            self._synchronous = True
            self._sequential = True
            self._asynchronous = False
        else:
            self._synchronous = False
            self._sequential = sequential
            self._asynchronous = True

        if not hasattr(self, 'num_rpc_threads'):
            if self._sequential:
                num_rpc_threads = 1
            self._num_rpc_threads = num_rpc_threads
        if not hasattr(self, 'num_main_threads'):
            if self._sequential:
                num_main_threads = 1
            self._num_main_threads = num_main_threads

        if self._sequential:
            if self._synchronous:
                self.logger.log('execution mode: synchronous (sequential)')
            else:
                self.logger.log('execution mode: asynchronous (sequential)')
        else:
            if self._synchronous:
                self.logger.log('execution mode: synchronous')
            else:
                self.logger.log('execution mode: asynchronous')

        if not hasattr(self, 'input_mode'):
            self._input_mode = input_mode

    def _set_consumer_timeout(self, consumer_timeout):
        if not hasattr(self, 'consumer_timeout'):
            self._consumer_timeout = consumer_timeout
        self._consumer_timeout = self._consumer_timeout * 1000

    def _set_input_topic_exp_assignments(self):
        self._input_topic_assignments = {}
        window_size = 900  # in seconds, 15 minutes
        topics_no = len(self._input_topics)
        self.logger.log('input topics time assingments:')
        for index, topic in enumerate(self._input_topics):
            topic_assingment = \
                self._get_index_assignment(window_size, index, topics_no)
            self._input_topic_assignments[topic] = topic_assingment
            self.logger.log(f' - {topic}: {topic_assingment} seconds')

    @staticmethod
    def in_time(start_time, assigned_time):
        return (start_time - utils.get_timestamp_ms()) < assigned_time

    @staticmethod
    def _parse_aerospike_args(parser):
        parser.add_argument('-a',
                            '--aerospike',
                            '--aerospike-bootstrap-server',
                            action="store",
                            dest="aerospike_host_port",
                            help='Aerospike bootstrap server. \
                            E.g., "localhost:3000"',
                            required=False)

    def _initialize_aerospike_attributes(self, parsed_args):
        if parsed_args.aerospike_host_port:
            aerospike_host_port = parsed_args.aerospike_host_port.split(':')
            self._aerospike_host = aerospike_host_port[0]
            self._aerospike_port = int(aerospike_host_port[1])

    @staticmethod
    def _parse_mongodb_args(parser):
        parser.add_argument('-m',
                            '--mongodb',
                            action="store",
                            dest="mongodb_host_port",
                            help='MongoDB server. \
                            E.g., "localhost:27017"',
                            required=False)

    def _initialize_mongodb_attributes(self, parsed_args):
        if parsed_args.mongodb_host_port:
            mongodb_host_port = parsed_args.mongodb_host_port.split(':')
            self._mongodb_host = mongodb_host_port[0]
            self._mongodb_port = int(mongodb_host_port[1])

    @staticmethod
    def _parse_kafka_args(parser):
        parser.add_argument('-i',
                            '--input',
                            action="store",
                            dest="input_topics",
                            help='Kafka input topics. Several topics ' + 'can be specified separated by commas',
                            required=False)
        parser.add_argument('-o',
                            '--output',
                            action="store",
                            dest="output_topics",
                            help='Kafka output topics. Several topics ' + 'can be specified separated by commas',
                            required=False)
        parser.add_argument('-k',
                            '--kafka-bootstrap-server',
                            action="store",
                            dest="kafka_endpoint",
                            help='Kafka bootstrap server. \
                            E.g., "localhost:9092"',
                            required=False)
        parser.add_argument('-g',
                            '--consumer-group',
                            action="store",
                            dest="consumer_group",
                            help='Kafka consumer group.',
                            required=False)
        parser.add_argument('--consumer-timeout',
                            action="store",
                            dest="consumer_timeout",
                            help='Kafka consumer timeout in seconds.',
                            required=False)

    def _initialize_kafka_attributes(self, args):
        if args.input_topics:
            self._input_topics = args.input_topics.split(',')
        else:
            self._input_topics = []

        if args.output_topics:
            self._output_topics = args.output_topics.split(',')
        else:
            self._output_topics = []

        self._kafka_endpoint = args.kafka_endpoint

        if args.consumer_group:
            self._consumer_group = args.consumer_group

        if args.consumer_timeout:
            self._consumer_timeout = args.consumer_timeout

    @staticmethod
    def _parse_catenae_args(parser):
        parser.add_argument('--log-level',
                            action="store",
                            dest="log_level",
                            help='Catenae log level [debug|info|warning|error|critical].',
                            required=False)
        parser.add_argument(
            '--running-mode',
            action="store",
            dest="link_mode",
            help=
            'Link running mode [0(MULTIPLE_KAFKA_INPUTS)|1(CUSTOM_OUTPUT)|2(MULTIPLE_KAFKA_INPUTS_CUSTOM_OUTPUT)|3(CUSTOM_INPUT)].',
            required=False)
        parser.add_argument('--input-mode',
                            action="store",
                            dest="input_mode",
                            help='Link input mode [parity|exp].',
                            required=False)
        parser.add_argument('--sync',
                            action="store_true",
                            dest="synchronous",
                            help='Synchronous mode is enabled.',
                            required=False)
        parser.add_argument('--seq',
                            action="store_true",
                            dest="sequential",
                            help='Sequential mode is enabled.',
                            required=False)
        parser.add_argument('--async',
                            action="store_true",
                            dest="asynchronous",
                            help='Synchronous mode is disabled.',
                            required=False)
        parser.add_argument('--random-consumer-group',
                            action="store_true",
                            dest="random_consumer_group",
                            help='Synchronous mode is disabled.',
                            required=False)
        parser.add_argument('--rpc-threads',
                            action="store",
                            dest="num_rpc_threads",
                            help='Number of RPC threads.',
                            required=False)
        parser.add_argument('--main-threads',
                            action="store",
                            dest="num_main_threads",
                            help='Number of main threads.',
                            required=False)

    def _initialize_catenae_attributes(self, args):
        if args.log_level:
            self._log_level = args.log_level
        if args.link_mode:
            self._link_mode = args.link_mode
        if args.input_mode:
            self._input_mode = args.input_mode
        if args.synchronous:
            self._synchronous = True
        if args.sequential:
            self._sequential = True
        if args.asynchronous:
            self._asynchronous = True
        if args.random_consumer_group:
            self._random_consumer_group = True
        if args.num_rpc_threads:
            self._num_rpc_threads = args.num_rpc_threads
        if args.num_main_threads:
            self._num_main_threads = args.num_main_threads

    def _load_args(self):
        parser = argparse.ArgumentParser()

        Link._parse_catenae_args(parser)
        Link._parse_kafka_args(parser)
        Link._parse_aerospike_args(parser)
        Link._parse_mongodb_args(parser)

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        self._initialize_catenae_attributes(link_args)
        self._initialize_kafka_attributes(link_args)
        self._initialize_aerospike_attributes(link_args)
        self._initialize_mongodb_attributes(link_args)