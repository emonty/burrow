# Copyright (C) 2011 OpenStack Foundation
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

import urlparse

import burrow.common
import burrow.config
from burrow.openstack.common import importutils


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
        self.config = burrow.config.Config(self._config, 'burrow.client')
        if url is not None:
            self._parse_url(url)
        self.log = burrow.common.get_logger(self.config)
        if len(self.log.handlers) == 0 and add_default_log_handler:
            burrow.common.add_default_log_handler()
        self.backend = self._import_backend()

    def _parse_url(self, url):
        '''Parse a backend URL and set config values so it overrides
        previous values.'''
        backend = 'burrow.backend.' + urlparse.urlparse(url).scheme
        self.config.set('backend', backend)
        if not self._config.has_section(backend):
            self._config.add_section(backend)
        self._config.set(backend, 'url', url)

    def _import_backend(self):
        '''Load backend given in the 'backend' option.'''
        backend = self.config.get('backend', DEFAULT_BACKEND)
        config = (self._config, backend)
        return importutils.import_class(backend, 'Backend')(config)

    def __getattr__(self, name):
        return getattr(self.backend, name)


class Account(object):
    '''Convenience wrapper around the Client class that saves the
    account setting. This allows you to use methods without specifying
    the 'account' parameter every time.'''

    account_methods = [
        'delete_queues',
        'get_queues',
        'delete_messages',
        'get_messages',
        'update_messages',
        'create_message',
        'delete_message',
        'get_message',
        'update_message']

    def __init__(self, account, client=None, **kwargs):
        self.account = account
        if client is None:
            self.client = Client(**kwargs)
        else:
            self.client = client

    def __getattr__(self, name):
        '''If the requested method is an account method, return a
        wrapper with the given account parameters.'''
        if name not in self.account_methods:
            return getattr(self.client, name)

        def function(*args, **kwargs):
            '''Call the client method with the account.'''
            return getattr(self.client, name)(self.account, *args, **kwargs)
        return function


class Queue(Account):
    '''Convenience wrapper around the Client class that saves the
    account and queue setting. This allows you to use methods without
    specifying the 'account' and 'queue' parameter every time.'''

    def __init__(self, account, queue, **kwargs):
        super(Queue, self).__init__(account, **kwargs)
        self.queue = queue
        self.queue_methods = self.account_methods[2:]

    def __getattr__(self, name):
        '''If the requested method is a queue method, return a wrapper
        with the given account and queue parameters.'''
        if name not in self.queue_methods:
            return super(Queue, self).__getattr__(name)

        def function(*args, **kwargs):
            '''Call the client method with the account and queue.'''
            return getattr(self.client, name)(self.account, self.queue, *args,
                **kwargs)
        return function
