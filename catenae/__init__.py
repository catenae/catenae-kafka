#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from .electron import Electron
from .link import Link
from . import utils
from .structures import CircularOrderedDict, CircularOrderedSet
from .custom_queue import ThreadingQueue
from .custom_threading import Thread, ThreadPool

__version__ = '1.0.0'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

catenae_logo = '\n\n         xxx            xx     xxxxxxxxxxxxxxxxxx     xxx         xxx       xxx           xx               xxx\n       xxx             xxxx            xx           xxx           xxxx      xxx          xxxx            xxx\n     xxx             xxx  xxx          xx         xxx             xxxxx     xxx        xxx  xxx        xxx\n   xxx              xxx    xxx         xx       xxx               xxx xxx   xxx       xxx    xxx     xxx\n xxx               xxx      xxx        xx     xxxxxxxxxxxxxxxxxx  xxx  xxx  xxx      xxx      xxx   xxxxxxxxxxxxxxxxxx\n   xxx            xxx        xxx       xx       xxx               xxx    xx xxx     xxx        xxx    xxx\n     xxx         xxx          xxx      xx         xxx             xxx     xxxxx    xxx          xxx     xxx\n       xxx      xxx            xxx     xx           xxx           xxx      xxxx   xxx            xxx      xxx\n         xxx   xxx              xxx    xx             xxx         xxx       xxx  xxx              xxx       xxx\n'
logging.info(catenae_logo)
logging.info(f'Catenae v{__version__} Valence\n')
