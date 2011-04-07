# Copyright (C) 2011 OpenStack LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Main module for burrow.'''

import gettext
import logging
import sys

from burrow.client import Client
from burrow.server import Server
import burrow.config

__version__ = '2011.2'

# This installs the _(...) function as a built-in so all other modules
# don't need to.
gettext.install('burrow')


class Module(object):
    '''Common module class for burrow.'''

    def __init__(self, config):
        self.config = burrow.config.Config(*config)
        self.log = get_logger(self.config)
        self.log.debug(_('Module created'))


def get_logger(config):
    '''Create a logger from the given config.'''
    log = logging.getLogger(config.section)
    log_level = config.get('log_level', 'DEBUG')
    log_level = logging.getLevelName(log_level)
    if isinstance(log_level, int):
        log.setLevel(log_level)
    return log


def import_class(module_name, class_name=None):
    '''Import a class given a full module.class name.'''
    if class_name is None:
        module_name, _separator, class_name = module_name.rpartition('.')
    try:
        __import__(module_name)
        return getattr(sys.modules[module_name], class_name)
    except (ImportError, ValueError, AttributeError), exception:
        raise ImportError(_('Class %s.%s cannot be found (%s)') %
            (module_name, class_name, exception))
