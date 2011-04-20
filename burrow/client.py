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

import burrow.common
import burrow.config

# Default configuration values for this module.
DEFAULT_BACKEND = 'burrow.backend.http'


class Client(object):
    '''Client class for burrow.'''

    def __init__(self, url=None, config_files=None,
        add_default_log_handler=True):
        '''Initialize a client using the URL and config files from the
        given list. This is passed directly to ConfigParser.read(),
        so files should be in ConfigParser format. This will load
        all the backend class from the configuration.'''
        self._config = burrow.config.load_config_files(config_files)
        # TODO: Parse URL if given and overwrite any values in self._config.
        self.config = burrow.config.Config(self._config, 'burrow.client')
        self.log = burrow.common.get_logger(self.config)
        if len(self.log.handlers) == 0 and add_default_log_handler:
            burrow.common.add_default_log_handler()
        self.backend = self._import_backend()

    def _import_backend(self):
        '''Load backend given in the 'backend' option.'''
        backend = self.config.get('backend', DEFAULT_BACKEND)
        config = (self._config, backend)
        return burrow.common.import_class(backend, 'Backend')(config)
