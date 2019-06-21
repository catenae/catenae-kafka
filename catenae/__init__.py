#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from .electron import Electron
from .link import Link
from . import utils
from .structures import CircularOrderedDict, CircularOrderedSet
from .custom_queue import ThreadingQueue
from .custom_threading import Thread, ThreadPool

__version__ = '1.0.0rc2'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

logging.info(f'Catenae v{__version__}')
