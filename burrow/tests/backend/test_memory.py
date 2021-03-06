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

'''Unittests for the memory backend.'''

import ConfigParser

import burrow.backend.memory
from burrow.tests import backend


class MemoryBase(backend.Base):
    '''Base test case for memory backend.'''

    def setUp(self):
        super(MemoryBase, self).setUp()
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = burrow.backend.memory.Backend(config)
        self.check_empty()


class TestMemoryAccounts(MemoryBase, backend.TestAccounts):
    '''Test case for accounts with memory backend.'''
    pass


class TestMemoryQueues(MemoryBase, backend.TestQueues):
    '''Test case for queues with memory backend.'''
    pass


class TestMemoryMessages(MemoryBase, backend.TestMessages):
    '''Test case for messages with memory backend.'''
    pass


class TestMemoryMessage(MemoryBase, backend.TestMessage):
    '''Test case for message with memory backend.'''
    pass
