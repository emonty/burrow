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

'''Unittests for the HTTP backend. This starts the WSGI server so
tests the WSGI frontend as well.'''

import ConfigParser

import burrow.backend.http
import test.backend


class HTTPBase(test.backend.Base):
    '''Base test case for http backend.'''

    def setUp(self):
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = burrow.backend.http.Backend(config)
        self.check_empty()


class TestHTTPAccounts(HTTPBase, test.backend.TestAccounts):
    '''Test case for accounts with http backend.'''
    pass


class TestHTTPQueues(HTTPBase, test.backend.TestQueues):
    '''Test case for queues with http backend.'''
    pass


class TestHTTPMessages(HTTPBase, test.backend.TestMessages):
    '''Test case for messages with http backend.'''
    pass


class TestHTTPMessage(HTTPBase, test.backend.TestMessage):
    '''Test case for message with http backend.'''
    pass
