#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from .electron import Electron
from .link import Link, rpc
from . import utils
from . import errors
from .structures import CircularOrderedDict, CircularOrderedSet
from .custom_queue import ThreadingQueue
from .custom_threading import Thread, ThreadPool

text_logo = '\n\n         ◼◼◼            ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼     ◼◼◼         ◼◼◼       ◼◼◼           ◼◼                ◼◼◼\n       ◼◼◼             ◼◼◼◼            ◼◼           ◼◼◼           ◼◼◼◼      ◼◼◼          ◼◼◼◼             ◼◼◼\n     ◼◼◼             ◼◼◼  ◼◼◼          ◼◼         ◼◼◼             ◼◼◼◼◼     ◼◼◼        ◼◼◼  ◼◼◼         ◼◼◼\n   ◼◼◼              ◼◼◼    ◼◼◼         ◼◼       ◼◼◼               ◼◼◼ ◼◼◼   ◼◼◼       ◼◼◼    ◼◼◼      ◼◼◼\n ◼◼◼               ◼◼◼      ◼◼◼        ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼  ◼◼◼  ◼◼◼  ◼◼◼      ◼◼◼      ◼◼◼   ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼\n   ◼◼◼            ◼◼◼        ◼◼◼       ◼◼       ◼◼◼               ◼◼◼    ◼◼ ◼◼◼     ◼◼◼        ◼◼◼    ◼◼◼\n     ◼◼◼         ◼◼◼          ◼◼◼      ◼◼         ◼◼◼             ◼◼◼     ◼◼◼◼◼    ◼◼◼          ◼◼◼     ◼◼◼\n       ◼◼◼      ◼◼◼            ◼◼◼     ◼◼           ◼◼◼           ◼◼◼      ◼◼◼◼   ◼◼◼            ◼◼◼      ◼◼◼\n         ◼◼◼   ◼◼◼              ◼◼◼    ◼◼             ◼◼◼         ◼◼◼       ◼◼◼  ◼◼◼              ◼◼◼       ◼◼◼\n'
__version__ = '2.0.0a0'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')