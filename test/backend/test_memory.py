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

import ConfigParser
import time
import unittest

import eventlet

import burrow.backend.memory


class TestMemory(unittest.TestCase):
    '''Unittests for the memory backend.'''
    backend_class = burrow.backend.memory.Backend

    def setUp(self):
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = self.backend_class(config)
        self.assertEquals([], list(self.backend.get_accounts()))
        self.assertEquals([], list(self.backend.get_queues('a')))
        self.assertEquals([], list(self.backend.get_messages('a', 'q')))

    def tearDown(self):
        self.assertEquals([], list(self.backend.get_messages('a', 'q')))
        self.assertEquals([], list(self.backend.get_queues('a')))
        self.assertEquals([], list(self.backend.get_accounts()))

    def test_account(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='all')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals(['a'], accounts)

    def test_account_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='id')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals(['a'], accounts)

    def test_account_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='none')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals([], accounts)

    def test_account_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='bad')
        accounts = self.backend.delete_accounts(filters)
        self.assertRaises(burrow.backend.BadDetail, list, accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='all')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals(['a'], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='id')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals(['a'], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='none')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals([], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='bad')
        accounts = self.backend.get_accounts(filters)
        self.assertRaises(burrow.backend.BadDetail, list, accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_marker(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(marker=accounts[0])
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[1:], accounts2)
        filters = dict(marker=accounts[1])
        accounts3 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[2:], accounts3)
        filters = dict(marker=accounts[2])
        accounts4 = list(self.backend.get_accounts(filters))
        self.assertEquals([], accounts4)
        self.assertEquals([], list(self.backend.delete_accounts()))
