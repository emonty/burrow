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

'''Common classes and functions for burrow.'''

import logging
import sys

import burrow.config


class Module(object):
    '''Common module class for burrow. This manages setting up
    configuration and logging for a module object.'''

    def __init__(self, config):
        self.config = burrow.config.Config(*config)
        self.log = get_logger(self.config)
        self.log.debug(_('Module created'))


def get_logger(config):
    '''Create a logger from the given config using the section name
    and optional log level.'''
    log = logging.getLogger(config.section)
    log_level = config.get('log_level', 'DEBUG')
    log_level = logging.getLevelName(log_level)
    if isinstance(log_level, int):
        log.setLevel(log_level)
    return log


def add_default_log_handler():
    '''Add a default log handler if one has not been set.'''
    root_log = logging.getLogger()
    if len(root_log.handlers) > 0:
        return
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler.setFormatter(logging.Formatter(log_format))
    root_log.addHandler(handler)


def import_class(module_name, class_name=None):
    '''Import a class given a full module.class name or seperate
    module and options. If no class_name is given, it is assumed to
    be the last part of the module_name string.'''
    if class_name is None:
        module_name, _separator, class_name = module_name.rpartition('.')
    try:
        __import__(module_name)
        return getattr(sys.modules[module_name], class_name)
    except (ImportError, ValueError, AttributeError), exception:
        raise ImportError(_('Class %s.%s cannot be found (%s)') %
            (module_name, class_name, exception))
