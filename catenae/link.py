#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#          ◼◼◼            ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼     ◼◼◼         ◼◼◼       ◼◼◼           ◼◼                ◼◼◼
#        ◼◼◼             ◼◼◼◼            ◼◼           ◼◼◼           ◼◼◼◼      ◼◼◼          ◼◼◼◼             ◼◼◼
#      ◼◼◼             ◼◼◼  ◼◼◼          ◼◼         ◼◼◼             ◼◼◼◼◼     ◼◼◼        ◼◼◼  ◼◼◼         ◼◼◼
#    ◼◼◼              ◼◼◼    ◼◼◼         ◼◼       ◼◼◼               ◼◼◼ ◼◼◼   ◼◼◼       ◼◼◼    ◼◼◼      ◼◼◼
#  ◼◼◼               ◼◼◼      ◼◼◼        ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼  ◼◼◼  ◼◼◼  ◼◼◼      ◼◼◼      ◼◼◼   ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼
#    ◼◼◼            ◼◼◼        ◼◼◼       ◼◼       ◼◼◼               ◼◼◼    ◼◼ ◼◼◼     ◼◼◼        ◼◼◼    ◼◼◼
#      ◼◼◼         ◼◼◼          ◼◼◼      ◼◼         ◼◼◼             ◼◼◼     ◼◼◼◼◼    ◼◼◼          ◼◼◼     ◼◼◼
#        ◼◼◼      ◼◼◼            ◼◼◼     ◼◼           ◼◼◼           ◼◼◼      ◼◼◼◼   ◼◼◼            ◼◼◼      ◼◼◼
#          ◼◼◼   ◼◼◼              ◼◼◼    ◼◼             ◼◼◼         ◼◼◼       ◼◼◼  ◼◼◼              ◼◼◼       ◼◼◼
#
# Catenae 1.0.0 Valence
# Copyright (C) 2017-2019 Rodrigo Martínez Castaño
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import math
from threading import Lock, current_thread
from multiprocessing import Pipe
from pickle5 import pickle
import time
import argparse
from uuid import uuid4
from os import environ
from confluent_kafka import Producer, Consumer, KafkaError
import signal
from . import utils
from .electron import Electron
from .callback import Callback
from .logger import Logger
from .custom_queue import ThreadingQueue
from .custom_threading import Thread, ThreadPool
from .custom_multiprocessing import Process
from .connectors.aerospike import AerospikeConnector
from .connectors.mongodb import MongodbConnector
from .structures import CircularOrderedSet


class Link:
    def __init__(self,
                 log_level='INFO',
                 input_mode='parity',
                 exp_window_size=900,
                 synchronous=False,
                 sequential=False,
                 uid_consumer_group=False,
                 num_rpc_threads=1,
                 num_main_threads=1,
                 input_topics=None,
                 output_topics=None,
                 kafka_endpoint='localhost:9092',
                 consumer_group=None,
                 consumer_timeout=300,
                 aerospike_endpoint=None,
                 mongodb_endpoint=None):

        # Preserve the id if the container restarts
        if 'CATENAE_DOCKER' in environ \
        and bool(environ['CATENAE_DOCKER']):
            self._uid = environ['HOSTNAME']
        else:
            self._uid = utils.keccak256(str(uuid4()))[:12]

        self._set_log_level(log_level)
        self.logger = Logger(self, self._log_level)
        self.logger.log(f'log level: {self._log_level}')

        self._started = False
        self._stopped = False
        self._input_topics_lock = Lock()
        self._rpc_lock = Lock()
        self._start_stop_lock = Lock()

        # RPC topics
        self.rpc_instance_topic = f'catenae_rpc_{self._uid}'
        self.rpc_group_topic = f'catenae_rpc_{self.__class__.__name__.lower()}'
        self.rpc_broadcast_topic = 'catenae_rpc_broadcast'
        self._rpc_topics = [self.rpc_instance_topic, self.rpc_group_topic, self.rpc_broadcast_topic]
        self._known_message_ids = CircularOrderedSet(50)

        self._load_args()
        self._set_execution_opts(input_mode, exp_window_size, synchronous, sequential, num_rpc_threads,
                                 num_main_threads, input_topics, output_topics, kafka_endpoint, consumer_timeout)
        self._set_connectors_properties(aerospike_endpoint, mongodb_endpoint)
        self._set_consumer_group(consumer_group, uid_consumer_group)

        self._input_messages = ThreadingQueue()
        self._output_messages = ThreadingQueue()

        self._changed_input_topics = False

    TIMEOUT = 0.5
    COMMIT_ATTEMPTS = 30

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

    def start(self, embedded=False):
        with self._start_stop_lock:
            if self._started:
                return
            self._started = True

        if self._kafka_endpoint:
            self._set_kafka_common_properties()
            self._setup_kafka_producers()
        self._set_connectors()

        try:
            self.setup()
            self.logger.log(f'link {self._uid} is starting...')
            self._launch_tasks()
        except Exception:
            try:
                self.suicide('Exception during the execution of setup()', exception=True)
            except SystemExit:
                pass
        finally:
            self.logger.log(f'link {self._uid} is running')
            if embedded:
                Thread(target=self._join_tasks).start()
            else:
                self._setup_signals_handler()
                self._join_tasks()

    def _join_tasks(self):
        if hasattr(self, '_generator_main_thread'):
            self._generator_main_thread.join()
        
        if self._kafka_endpoint:
            if hasattr(self, '_producer_thread'):
                self._producer_thread.join()
            if hasattr(self, '_input_handler_thread'):
                self._input_handler_thread.join()
            if hasattr(self, '_consumer_rpc_thread'):
                self._consumer_rpc_thread.join()
            if hasattr(self, '_consumer_main_thread'):
                self._consumer_main_thread.join()

            if hasattr(self, '_transform_rpc_executor'):
                for i, thread in enumerate(self._transform_rpc_executor.threads):
                    self._join_if_not_current_thread(thread)
            if hasattr(self, '_transform_main_executor'):
                for i, thread in enumerate(self._transform_main_executor.threads):
                    self._join_if_not_current_thread(thread)

        self.logger.log(f'link {self.uid} stopped')

    def setup(self):
        pass

    def transform(self, _):
        for thread in self._transform_main_executor.threads:
            thread.stop()

    def finish(self):
        pass

    def generator(self):
        self.logger.log('Generator method undefined. Disabled.', level='debug')
        # Kill the generator thread
        raise SystemExit

    def send(self,
             output_content,
             topic=None,
             callback=None,
             callback_args=None,
             callback_kwargs=None,
             synchronous=None):
        if isinstance(output_content, Electron):
            if topic:
                output_content.topic = topic
            electron = output_content.copy()
        elif not isinstance(output_content, list):
            electron = Electron(value=output_content, topic=topic, unpack_if_string=True)
        else:
            for i, item in enumerate(output_content):
                # Last item includes the callback
                if i == len(output_content) - 1:
                    self.send(item, topic=topic, callback=callback, synchronous=synchronous)
                else:
                    self.send(item, topic=topic, synchronous=synchronous)
            return

        if callback is not None:
            electron.callbacks.append(Callback(callback, callback_args, callback_kwargs))

        if synchronous is None:
            synchronous = self._synchronous

        # Electrons can be sent asynchronously / synchronously individually
        if synchronous:
            self._produce(electron, synchronous=synchronous)
        else:
            self._output_messages.put(electron)

    def add_input_topic(self, input_topic):
        with self._input_topics_lock:
            if input_topic not in self._input_topics:
                self._input_topics.append(input_topic)
                if self._input_mode == 'exp':
                    self._set_input_topic_assignments()
                self._changed_input_topics = True
                self.logger.log(f'added input {input_topic}')

    def remove_input_topic(self, input_topic):
        with self._input_topics_lock:
            if input_topic in self._input_topics:
                self._input_topics.remove(input_topic)
                if self._input_mode == 'exp':
                    self._set_input_topic_assignments()
                self._changed_input_topics = True
                self.logger.log(f'removed input {input_topic}')

    def suicide(self, message=None, exception=False):
        with self._start_stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self.finish()

        if message is None:
            message = '[SUICIDE]'
        else:
            message = f'[SUICIDE] {message}'

        if exception:
            self.logger.log(message, level='exception')
        else:
            self.logger.log(message, level='warn')

        while not self._started:
            time.sleep(Link.TIMEOUT)

        if hasattr(self, '_generator_main_thread'):
            self._generator_main_thread.stop()

        self.logger.log('stopping threads...')
        if self._kafka_endpoint:
            if hasattr(self, '_producer_thread'):
                self._producer_thread.stop()
            if hasattr(self, '_input_handler_thread'):
                self._input_handler_thread.stop()
            if hasattr(self, '_consumer_rpc_thread'):
                self._consumer_rpc_thread.stop()
            if hasattr(self, '_consumer_main_thread'):
                self._consumer_main_thread.stop()

            if hasattr(self, '_transform_rpc_executor'):
                for thread in self._transform_rpc_executor.threads:
                    thread.stop()
            if hasattr(self, '_transform_main_executor'):
                for thread in self._transform_main_executor.threads:
                    thread.stop()

        # Kill the thread that invoked the suicide method
        raise SystemExit

    def loop(self, target, args=None, kwargs=None, interval=0, wait=False):
        loop_task_kwargs = {'target': target, 'args': args, 'kwargs': kwargs, 'interval': interval, 'wait': wait}
        loop_thread = Thread(target=self._loop_task, kwargs=loop_task_kwargs)
        loop_thread.start()
        return loop_thread

    def launch_thread(self, target, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        thread = Thread(target=target, args=args, kwargs=kwargs)
        thread.start()
        return thread

    def launch_process(self, target, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        process = Process(target=target, args=args, kwargs=kwargs)
        process.start()
        return process

    def _set_execution_opts(self, input_mode, exp_window_size, synchronous, sequential, num_rpc_threads,
                            num_main_threads, input_topics, output_topics, kafka_endpoint, consumer_timeout):

        if not hasattr(self, '_input_mode'):
            self._input_mode = input_mode
        self.logger.log(f'input_mode: {self._input_mode}')

        if not hasattr(self, '_exp_window_size'):
            self._exp_window_size = exp_window_size
            if self._input_mode == 'exp':
                self.logger.log(f'exp_window_size: {self._exp_window_size}')

        if hasattr(self, '_synchronous'):
            synchronous = self._synchronous

        if hasattr(self, '_sequential'):
            sequential = self._sequential

        if synchronous:
            self._synchronous = True
            self._sequential = True
        else:
            self._synchronous = False
            self._sequential = sequential

        if self._synchronous:
            self.logger.log('execution mode: sync + seq')
        else:
            if self._sequential:
                self.logger.log('execution mode: async + seq')
            else:
                self.logger.log('execution mode: async')

        if hasattr(self, '_num_rpc_threads'):
            num_rpc_threads = self._num_rpc_threads

        if hasattr(self, '_num_main_threads'):
            num_main_threads = self._num_main_threads

        if synchronous or sequential:
            self._num_main_threads = 1
            self._num_rpc_threads = 1
        else:
            self._num_rpc_threads = num_rpc_threads
            self._num_main_threads = num_main_threads
        self.logger.log(f'num_rpc_threads: {self._num_rpc_threads}')
        self.logger.log(f'num_main_threads: {self._num_main_threads}')

        if not hasattr(self, '_input_topics'):
            self._input_topics = input_topics
        self.logger.log(f'input_topics: {self._input_topics}')

        if not hasattr(self, '_output_topics'):
            self._output_topics = output_topics
        self.logger.log(f'output_topics: {self._output_topics}')

        if not hasattr(self, '_kafka_endpoint'):
            self._kafka_endpoint = kafka_endpoint
        self.logger.log(f'kafka_endpoint: {self._kafka_endpoint}')

        if not hasattr(self, '_consumer_timeout'):
            self._consumer_timeout = consumer_timeout
        self.logger.log(f'consumer_timeout: {self._consumer_timeout}')
        self._consumer_timeout = self._consumer_timeout * 1000

    def _set_connectors_properties(self, aerospike_endpoint, mongodb_endpoint):
        self._set_aerospike_properties(aerospike_endpoint)
        if hasattr(self, '_aerospike_host'):
            self.logger.log(f'aerospike_host: {self._aerospike_host}')
        if hasattr(self, '_aerospike_port'):
            self.logger.log(f'aerospike_port: {self._aerospike_port}')

        self._set_mongodb_properties(mongodb_endpoint)
        if hasattr(self, '_mongodb_host'):
            self.logger.log(f'mongodb_host: {self._mongodb_host}')
        if hasattr(self, '_mongodb_port'):
            self.logger.log(f'mongodb_port: {self._mongodb_port}')

    def _loop_task(self, target, args=None, kwargs=None, interval=None, wait=False):
        if wait:
            time.sleep(interval)

        if args is None:
            args = []

        if kwargs is None:
            kwargs = {}

        if not isinstance(args, list):
            args = [args]

        while not current_thread().will_stop:
            try:
                self.logger.log(f'new loop iteration ({target.__name__})', level='debug')
                start_timestamp = utils.get_timestamp()

                target(*args, **kwargs)

                sleep_seconds = interval - utils.get_timestamp() + start_timestamp
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            except Exception:
                self.logger.log(f'exception raised when executing the loop: {target.__name__}', level='exception')

    def _signal_handler(self, sig, frame):
        if sig == signal.SIGINT:
            signal_name = 'SIGINT'
        elif sig == signal.SIGTERM:
            signal_name = 'SIGINT'
        elif sig == signal.SIGQUIT:
            signal_name = 'SIGQUIT'
        self.suicide(f'({signal_name})')

    def rpc_call(self, to='broadcast', method=None, args=None, kwargs=None):
        """ 
        Send a Kafka message which will be interpreted as a RPC call by the receiver module.
        """
        if args is None:
            args = []

        if kwargs is None:
            kwargs = {}

        if not isinstance(args, list):
            args = [args]

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
        self.send(electron, synchronous=True)

    def _rpc_call(self, electron, commit_callback):
        with self._rpc_lock:
            if not 'method' in electron.value:
                self.logger.log(f'invalid RPC invocation: {electron.value}', level='error')
                return

            try:
                context = electron.value['context']
                self.logger.log(f"RPC invocation from {context['uid']} ({context['group']})", level='debug')

                args = [context] + electron.value['args']
                kwargs = electron.value['kwargs']
                getattr(self, electron.value['method'])(*args, **kwargs)

            except Exception:
                self.logger.log(f'error when invoking {method} remotely', level='exception')

            finally:
                commit_callback.execute()

    def _join_if_not_current_thread(self, thread):
        if thread is not current_thread():
            thread.join()

    def _kafka_producer(self):
        while not current_thread().will_stop:
            try:
                electron = self._output_messages.get(timeout=Link.TIMEOUT, block=False)
            except ThreadingQueue.EmptyError:
                continue
            self._produce(electron)

    def _produce(self, electron, synchronous=None):
        # All the queue items of the _output_messages must be individual
        # instances of Electron
        if not isinstance(electron, Electron):
            raise ValueError

        # The key is enconded for its use as partition key
        partition_key = None
        if electron.key:
            if isinstance(electron.key, str):
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
                self.suicide('electron / default output topic unset')
            electron.topic = self._output_topics[0]

        # Electrons are serialized
        if electron.unpack_if_string and isinstance(electron.value, str):
            serialized_electron = electron.value
        else:
            serialized_electron = pickle.dumps(electron.get_sendable(), protocol=pickle.HIGHEST_PROTOCOL)

        if synchronous is None:
            synchronous = self._synchronous

        if synchronous:
            producer = self._sync_producer
        else:
            producer = self._async_producer

        try:
            # If partition_key = None, the partition.assignment.strategy
            # is used to distribute the messages
            producer.produce(topic=electron.topic, key=partition_key, value=serialized_electron)

            if synchronous:
                # Wait for all messages in the Producer queue to be delivered.
                producer.flush()
            else:
                producer.poll(0)

            self.logger.log('electron produced', level='debug')

            for callback in electron.callbacks:
                callback.execute()

        except Exception:
            self.suicide('Kafka producer error', exception=True)

    def _transform(self, electron, commit_callback):
        with self._rpc_lock:
            try:
                transform_result = self.transform(electron)
                self.logger.log('electron transformed', level='debug')
            except Exception:
                self.suicide('exception during the execution of "transform"', exception=True)

            transform_callback = Callback()

            if not isinstance(transform_result, tuple):
                electrons = transform_result
            else:
                electrons = transform_result[0]
                if len(transform_result) > 1:
                    transform_callback.target = transform_result[1]
                    if len(transform_result) > 2:
                        if isinstance(transform_result[2], dict):
                            transform_callback.kwargs = transform_result[2]
                        else:
                            transform_callback.args = transform_result[2]

            # Transform returns None
            if electrons is None:
                if transform_callback:
                    transform_callback.execute()
                if commit_callback:
                    commit_callback.execute()
                return

            # Already a list
            if isinstance(electrons, list):
                real_electrons = []
                for electron in electrons:
                    if isinstance(electron, Electron):
                        real_electrons.append(electron)
                    else:
                        real_electrons.append(Electron(value=electron, unpack_if_string=True))
                electrons = real_electrons
            else:  # If there is only one item, convert it to a list
                if isinstance(electrons, Electron):
                    electrons = [electrons]
                else:
                    electrons = [Electron(value=electrons)]

            # Execute transform_callback only for the last electron
            if transform_callback:
                electrons[-1].callbacks.append(transform_callback)
            if commit_callback:
                electrons[-1].callbacks.append(commit_callback)

            for electron in electrons:
                if self._synchronous:
                    self._produce(electron)
                else:
                    self._output_messages.put(electron)

    def _input_handler(self):
        while not current_thread().will_stop:
            self.logger.log('waiting for a new electron to transform...', level='debug')

            try:
                queue_item = self._input_messages.get(timeout=Link.TIMEOUT, block=False)
            except ThreadingQueue.EmptyError:
                continue

            commit_callback = Callback(mode=Callback.COMMIT_KAFKA_MESSAGE)

            # Tuple
            if isinstance(queue_item, tuple):
                commit_callback.target = queue_item[1]
                if len(queue_item) > 2:
                    if isinstance(queue_item[2], list):
                        commit_callback.args = queue_item[2]
                    elif isinstance(queue_item[2], dict):
                        commit_callback.kwargs = queue_item[2]
                message = queue_item[0]
            else:
                message = queue_item

            if self._is_message_known(message):
                continue

            self._mark_known_message(message)
            self.logger.log('electron received', level='debug')

            try:
                electron = Electron(value=message.value().decode('utf-8'))
            except Exception:
                electron = pickle.loads(message.value())

            # Add the message timestamp
            message_timestamp = message.timestamp()[1]
            electron.timestamp = message_timestamp

            # Clean the previous topic
            electron.previous_topic = message.topic()
            electron.topic = None

            # The destiny topic will be overwritten if desired in the
            # transform method (default, first output topic)
            if electron.previous_topic in self._rpc_topics:
                self._transform_rpc_executor.submit(self._rpc_call, [electron, commit_callback])
            else:
                self._transform_main_executor.submit(self._transform, [electron, commit_callback])

    @staticmethod
    def _get_message_id(message):
        message_id = f'{message.topic()}_{message.partition()}_{message.offset()}'
        return message_id

    def _is_message_known(self, message):
        """ Avoid processing repeated messages. This is not mandatory for RPC 
        calls / synchronous mode"""
        message_id = Link._get_message_id(message)
        if message_id in self._known_message_ids:
            self.logger.log(f'Received known message (topic_partition_offset): {message_id}', level='debug')
            return True
        return False

    def _mark_known_message(self, message):
        message_id = Link._get_message_id(message)
        self._known_message_ids.add(message_id)

    def _break_consumer_loop(self, subscription):
        return len(subscription) > 1 and self._input_mode != 'parity'

    def _commit_kafka_message(self, consumer, message):
        commited = False
        attempts = 0
        self.logger.log(f'trying to commit the message {message.value()}', level='debug')
        while not commited:
            if attempts > 1:
                self.logger.log(f'trying to commit a message (attempt {attempts}/30)', level='warn')
            try:
                consumer.commit(message=message, asynchronous=False)
                commited = True
            except Exception:
                self.logger.log(f'could not commit the message {message.value()}', level='exception')
                attempts += 1
                if attempts == Link.COMMIT_ATTEMPTS:
                    self.suicide()
                else:
                    time.sleep(1)

        self.logger.log(f'message {message.value()} commited', level='debug')

    def _kafka_rpc_consumer(self):
        properties = dict(self._kafka_consumer_synchronous_properties)
        consumer = Consumer(properties)
        self.logger.log(f'[RPC] consumer properties: {utils.dump_dict_pretty(properties)}', level='debug')
        subscription = list(self._rpc_topics)
        consumer.subscribe(subscription)
        self.logger.log(f'[RPC] listening on: {subscription}')

        while not current_thread().will_stop:
            try:
                message = consumer.poll(Link.TIMEOUT)

                if not message or (not message.key() and not message.value()):
                    if not self._break_consumer_loop(subscription):
                        continue
                    # New topic / restart if there are more topics or
                    # there aren't assigned partitions
                    break

                if message.error():
                    # End of partition is not an error
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.suicide(str(message.error()))

                # Commit when the transformation is commited
                self._input_messages.put((message, self._commit_kafka_message, [consumer, message]))

            except Exception:
                try:
                    consumer.close()
                finally:
                    self.logger.log('stopped RPC input')
                    self.suicide('Kafka consumer error', exception=True)

        consumer.close()

    def _kafka_main_consumer(self):
        if self._synchronous:
            properties = dict(self._kafka_consumer_synchronous_properties)
        else:
            properties = dict(self._kafka_consumer_common_properties)

        consumer = Consumer(properties)
        self.logger.log(f'[MAIN] consumer properties: {utils.dump_dict_pretty(properties)}', level='debug')

        while not current_thread().will_stop:
            if not self._input_topics:
                self.logger.log('No input topics, waiting...', level='debug')
                time.sleep(Link.TIMEOUT)
                continue

            with self._input_topics_lock:
                self._set_input_topic_assignments()
                current_input_topic_assignments = dict(self._input_topic_assignments)

            for topic in current_input_topic_assignments.keys():

                with self._input_topics_lock:
                    if self._changed_input_topics:
                        self._changed_input_topics = False
                        break

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
                    assigned_time = current_input_topic_assignments[topic]
                    restarted_time = False
                    while (assigned_time == -1 \
                           or assigned_time == self._exp_window_size \
                           or self._on_time(start_time, assigned_time)) \
                    and not current_thread().will_stop:

                        with self._input_topics_lock:
                            assigned_time = current_input_topic_assignments[topic]

                            # Subscribe to the topics again if input topics have changed
                            if self._changed_input_topics:
                                # _changed_input_topics is set to False in the
                                # outer loop so both loops are broken
                                break

                        message = consumer.poll(Link.TIMEOUT)

                        if not message or (not message.key() and not message.value()):
                            if not self._break_consumer_loop(subscription):
                                continue

                            # New topic / restart if there are more topics or
                            # there aren't assigned partitions
                            break

                        if message.error():
                            # End of partition is not an error
                            if message.error().code() == KafkaError._PARTITION_EOF:
                                if not restarted_time:
                                    start_time = utils.get_timestamp_ms()
                                    restarted_time = True
                                continue
                            else:
                                self.suicide(str(message.error()))

                        # else
                        if not restarted_time:
                            start_time = utils.get_timestamp_ms()
                            restarted_time = True

                        # Synchronous commit
                        if self._synchronous:
                            # Commit when the transformation is commited
                            self._input_messages.put((message, self._commit_kafka_message, [consumer, message]))
                            continue

                        # Asynchronous
                        self._input_messages.put(message)
                        continue

                except Exception:
                    try:
                        consumer.close()
                    finally:
                        self.logger.log('stopped main input')
                        self.suicide('Kafka consumer error', exception=True)

        consumer.close()

    def _on_time(self, start_time, assigned_time):
        return (utils.get_timestamp_ms() - start_time) < 1000 * assigned_time

    def _get_index_assignment(self, index, elements_no, base=1.7):
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

        return (index_assignment / aggregated_value) * self._exp_window_size

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

    def _loop_thread_target(self, target, args=None, kwargs=None):
        while not current_thread().will_stop:
            self._thread_target(target, args, kwargs)

    def _setup_signals_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGQUIT, self._signal_handler)

    def _setup_kafka_producers(self):
        sync_producer_properties = dict(self._kafka_producer_synchronous_properties)
        self._sync_producer = Producer(sync_producer_properties)
        self.logger.log(f'sync producer properties: {utils.dump_dict_pretty(sync_producer_properties)}', level='debug')

        async_producer_properties = dict(self._kafka_producer_common_properties)
        self._async_producer = Producer(async_producer_properties)
        self.logger.log(f'async producer properties: {utils.dump_dict_pretty(async_producer_properties)}',
                        level='debug')

    def _launch_tasks(self):
        # Generator
        self._generator_main_thread = Thread(target=self._loop_thread_target, kwargs={'target': self.generator})
        self._generator_main_thread.start()

        if self._kafka_endpoint:
            # Kafka producer
            producer_kwargs = {'target': self._kafka_producer}
            self._producer_thread = Thread(target=self._thread_target, kwargs=producer_kwargs)
            self._producer_thread.start()

            # Transform
            self._transform_rpc_executor = ThreadPool(self, self._num_rpc_threads)
            self._transform_main_executor = ThreadPool(self, self._num_main_threads)
            transform_kwargs = {'target': self._input_handler}
            self._input_handler_thread = Thread(target=self._thread_target, kwargs=transform_kwargs)
            self._input_handler_thread.start()

            # Kafka RPC consumer
            consumer_kwargs = {'target': self._kafka_rpc_consumer}
            self._consumer_rpc_thread = Thread(target=self._thread_target, kwargs=consumer_kwargs)
            self._consumer_rpc_thread.start()

            # Kafka main consumer
            self._consumer_main_thread = Thread(target=self._thread_target,
                                                kwargs={'target': self._kafka_main_consumer})
            self._consumer_main_thread.start()

    def _set_log_level(self, log_level):
        if not hasattr(self, '_log_level'):
            self._log_level = log_level.upper()

    def _set_connectors(self):
        try:
            self._aerospike = AerospikeConnector(self._aerospike_host, self._aerospike_port, connect=True)
        except AttributeError:
            self._aerospike = None

        try:
            self._mongodb = MongodbConnector(self._mongodb_host, self._mongodb_port, connect=True)
        except AttributeError:
            self._mongodb = None

    def _set_consumer_group(self, consumer_group, uid_consumer_group):
        if hasattr(self, 'consumer_group'):
            consumer_group = self._consumer_group
        if hasattr(self, 'uid_consumer_group'):
            uid_consumer_group = self._uid_consumer_group

        if uid_consumer_group:
            self._consumer_group = f'catenae_{self._uid}'
        elif consumer_group:
            self._consumer_group = consumer_group
        else:
            self._consumer_group = f'catenae_{self.__class__.__name__.lower()}'

        self.logger.log(f'consumer_group: {self._consumer_group}')

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
            'session.timeout.ms': 10000,  # heartbeat thread
            'max.poll.interval.ms': self._consumer_timeout,  # processing time
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
            'acks': 1,  # ACK from the leader
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
            'acks': 'all',
            'max.in.flight.requests.per.connection': 1,
            'batch.num.messages': 1,
            'enable.idempotence': True,
        })

    def _set_input_topic_assignments(self):
        if self._input_mode == 'parity':
            self._input_topic_assignments = {-1: -1}

        elif self._input_mode == 'exp':
            self._input_topic_assignments = {}

            if len(self._input_topics[0]) == 1:
                self._input_topic_assignments[self._input_topics[0]] = -1

            topics_no = len(self._input_topics)
            self.logger.log('input topics time assingments:', level='debug')
            for index, topic in enumerate(self._input_topics):
                topic_assingment = \
                    self._get_index_assignment(index, topics_no)
                self._input_topic_assignments[topic] = topic_assingment
                self.logger.log(f' * {topic}: {topic_assingment} seconds', level='debug')

    def _load_args(self):
        parser = argparse.ArgumentParser()

        Link._parse_catenae_args(parser)
        Link._parse_kafka_args(parser)
        Link._parse_aerospike_args(parser)
        Link._parse_mongodb_args(parser)

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        self._set_catenae_properties_from_args(link_args)
        self._set_kafka_properties_from_args(link_args)
        self._set_aerospike_properties(link_args.aerospike_endpoint)
        self._set_mongodb_properties(link_args.mongodb_endpoint)

    @staticmethod
    def _parse_catenae_args(parser):
        parser.add_argument('--log-level',
                            action="store",
                            dest="log_level",
                            help='Catenae log level [debug|info|warning|error|critical].',
                            required=False)
        parser.add_argument('--input-mode',
                            action="store",
                            dest="input_mode",
                            help='Link input mode [parity|exp].',
                            required=False)
        parser.add_argument('--exp-window-size',
                            action="store",
                            dest="exp_window_size",
                            help='Consumption window size in seconds for exp mode.',
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
        parser.add_argument('--random-consumer-group',
                            action="store_true",
                            dest="uid_consumer_group",
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

    @staticmethod
    def _parse_aerospike_args(parser):
        parser.add_argument('-a',
                            '--aerospike',
                            '--aerospike-bootstrap-server',
                            action="store",
                            dest="aerospike_endpoint",
                            help='Aerospike bootstrap server. \
                            E.g., "localhost:3000"',
                            required=False)

    @staticmethod
    def _parse_mongodb_args(parser):
        parser.add_argument('-m',
                            '--mongodb',
                            action="store",
                            dest="mongodb_endpoint",
                            help='MongoDB server. \
                            E.g., "localhost:27017"',
                            required=False)

    def _set_catenae_properties_from_args(self, args):
        if args.log_level:
            self._log_level = args.log_level
        if args.input_mode:
            self._input_mode = args.input_mode
        if args.exp_window_size:
            self._exp_window_size = args.exp_window_size
        if args.synchronous:
            self._synchronous = True
        if args.sequential:
            self._sequential = True
        if args.uid_consumer_group:
            self._uid_consumer_group = True
        if args.num_rpc_threads:
            self._num_rpc_threads = args.num_rpc_threads
        if args.num_main_threads:
            self._num_main_threads = args.num_main_threads

    def _set_kafka_properties_from_args(self, args):
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

    def _set_aerospike_properties(self, aerospike_endpoint):
        if aerospike_endpoint is None:
            return
        host_port = aerospike_endpoint.split(':')
        if not hasattr(self, '_aerospike_host'):
            self._aerospike_host = host_port[0]
        if not hasattr(self, '_aerospike_port'):
            self._aerospike_port = int(host_port[1])

    def _set_mongodb_properties(self, mongodb_endpoint):
        if mongodb_endpoint is None:
            return
        host_port = mongodb_endpoint.split(':')
        if not hasattr(self, '_mongodb_host'):
            self._mongodb_host = host_port[0]
        if not hasattr(self, '_mongodb_port'):
            self._mongodb_port = int(host_port[1])