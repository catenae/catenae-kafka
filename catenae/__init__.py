#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .electron import *
from .link import *
from .utils import CircularOrderedDict, CircularOrderedSet
from . import utils
import logging


__version__ = '0.2.0'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info(f'Catenae v{__version__}')
