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

'''Client module for burrow.'''

import ConfigParser
import logging
import logging.config

import burrow
import burrow.config

# Default configuration values for this module.
DEFAULT_BACKEND = 'burrow.backend.http'


class Client(object):
    '''Client class for burrow.'''

    def __init__(self, url=None, config_files=[],
        add_default_log_handler=True):
        '''Initialize a client using the URL and config files from the
        given list. This is passed directly to ConfigParser.read(),
        so files should be in ConfigParser format. This will load
        all the backend class from the configuration.'''
        if len(config_files) > 0:
            logging.config.fileConfig(config_files)
        self._config = ConfigParser.ConfigParser()
        self._config.read(config_files)
        # TODO: Parse URL if given and overwrite any values in self._config.
        self.config = burrow.config.Config(self._config, 'burrow.client')
        self.log = burrow.get_logger(self.config)
        if add_default_log_handler:
            self._add_default_log_handler()
        self._import_backend()

    def _add_default_log_handler(self):
        '''Add a default log handler it one has not been set.'''
        root_log = logging.getLogger()
        if len(root_log.handlers) > 0 or len(self.log.handlers) > 0:
            return
        handler = logging.StreamHandler()
        handler.setLevel(logging.ERROR)
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        handler.setFormatter(logging.Formatter(log_format))
        root_log.addHandler(handler)

    def _import_backend(self):
        '''Load backend given in the 'backend' option.'''
        backend = self.config.get('backend', DEFAULT_BACKEND)
        config = (self._config, backend)
        self.backend = burrow.import_class(backend, 'Backend')(config)
