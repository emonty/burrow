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

'''Main server module for burrow.'''

import ConfigParser
import gettext
import logging
import logging.config
import sys

import eventlet

import burrowd.config

# This installs the _(...) function as a built-in so all other modules
# don't need to.
gettext.install('burrow')

# Default configuration values for this module.
DEFAULT_BACKEND = 'burrowd.backend.sqlite'
DEFAULT_FRONTENDS = 'burrowd.frontend.wsgi'
DEFAULT_THREAD_POOL_SIZE = 1000


class Burrowd(object):
    '''Server class for burrow.'''

    def __init__(self, config_files=[], add_default_log_handler=True):
        '''Initialize a server using the config files from the given
        list. This is passed directly to ConfigParser.read(), so
        files should be in ConfigParser format. This will load all
        frontend and backend classes from the configuration.'''
        if len(config_files) > 0:
            logging.config.fileConfig(config_files)
        self._config = ConfigParser.ConfigParser()
        self._config.read(config_files)
        self.config = burrowd.config.Config(self._config, 'burrowd')
        self.log = get_logger(self.config)
        if add_default_log_handler:
            self._add_default_log_handler()
        self._import_backend()
        self._import_frontends()

    def _add_default_log_handler(self):
        '''Add a default log handler it one has not been set.'''
        root_log = logging.getLogger()
        if len(root_log.handlers) > 0 or len(self.log.handlers) > 0:
            return
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        handler.setFormatter(logging.Formatter(log_format))
        root_log.addHandler(handler)

    def _import_backend(self):
        '''Load backend given in the 'backend' option.'''
        backend = self.config.get('backend', DEFAULT_BACKEND)
        config = (self._config, backend)
        self.backend = import_class(backend, 'Backend')(config)

    def _import_frontends(self):
        '''Load frontends given in the 'frontends' option.'''
        self.frontends = []
        frontends = self.config.get('frontends', DEFAULT_FRONTENDS)
        for frontend in frontends.split(','):
            frontend = frontend.split(':')
            if len(frontend) == 1:
                frontend.append(None)
            config = (self._config, frontend[0], frontend[1])
            frontend = import_class(frontend[0], 'Frontend')
            frontend = frontend(config, self.backend)
            self.frontends.append(frontend)

    def run(self):
        '''Create the thread pool and start the main server loop. Wait
        for the pool to complete, but possibly run forever if the
        frontends and backend never remove threads.'''
        thread_pool_size = self.config.getint('thread_pool_size',
            DEFAULT_THREAD_POOL_SIZE)
        thread_pool = eventlet.GreenPool(size=int(thread_pool_size))
        self.backend.run(thread_pool)
        for frontend in self.frontends:
            frontend.run(thread_pool)
        self.log.info(_('Waiting for all threads to exit'))
        try:
            thread_pool.waitall()
        except KeyboardInterrupt:
            pass


class Module(object):
    '''Common module class for burrow.'''

    def __init__(self, config):
        self.config = burrowd.config.Config(*config)
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
