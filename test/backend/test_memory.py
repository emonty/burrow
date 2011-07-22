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
import unittest

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
        self.assertEquals([dict(id='a')], accounts)

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

    def test_account_delete_marker(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', marker=accounts[0])
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:], accounts2)
        accounts3 = list(self.backend.get_accounts())
        self.assertEquals(accounts[:1], accounts3)
        filters = dict(detail='id', marker='unknown')
        accounts4 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[:1], accounts4)

    def test_account_delete_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', limit=1)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[:1], accounts2)
        filters = dict(detail='id', limit=2)
        accounts3 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:3], accounts3)

    def test_account_delete_marker_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', marker=accounts[1], limit=1)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[2:3], accounts2)
        filters = dict(detail='id', marker=accounts[0], limit=2)
        accounts3 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:2], accounts3)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        filters = dict(detail='all')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals([dict(id='a')], accounts)
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
        filters = dict(marker='unknown')
        accounts5 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts5)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(limit=1)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[:1], accounts2)
        filters = dict(limit=2)
        accounts3 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[:2], accounts3)
        filters = dict(limit=3)
        accounts4 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts4)
        filters = dict(limit=100)
        accounts5 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts5)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_account_get_marker_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(marker=accounts[1], limit=1)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[2:3], accounts2)
        filters = dict(marker=accounts[0], limit=2)
        accounts3 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[1:3], accounts3)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_queue(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='all')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals([dict(id='q')], queues)

    def test_queue_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='id')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals(['q'], queues)

    def test_queue_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='none')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals([], queues)

    def test_queue_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='bad')
        queues = self.backend.delete_queues('a', filters)
        self.assertRaises(burrow.backend.BadDetail, list, queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_delete_marker(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', marker=queues[0])
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:], queues2)
        queues3 = list(self.backend.get_queues('a'))
        self.assertEquals(queues[:1], queues3)
        filters = dict(detail='id', marker='unknown')
        queues4 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[:1], queues4)

    def test_queue_delete_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', limit=1)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[:1], queues2)
        filters = dict(detail='id', limit=2)
        queues3 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:3], queues3)

    def test_queue_delete_marker_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', marker=queues[1], limit=1)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[2:3], queues2)
        filters = dict(detail='id', marker=queues[0], limit=2)
        queues3 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:2], queues3)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='all')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals([dict(id='q')], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='id')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals(['q'], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='none')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals([], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        filters = dict(detail='bad')
        queues = self.backend.get_queues('a', filters)
        self.assertRaises(burrow.backend.BadDetail, list, queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_marker(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(marker=queues[0])
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[1:], queues2)
        filters = dict(marker=queues[1])
        queues3 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[2:], queues3)
        filters = dict(marker=queues[2])
        queues4 = list(self.backend.get_queues('a', filters))
        self.assertEquals([], queues4)
        filters = dict(marker='unknown')
        queues5 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues5)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(limit=1)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[:1], queues2)
        filters = dict(limit=2)
        queues3 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[:2], queues3)
        filters = dict(limit=3)
        queues4 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues4)
        filters = dict(limit=100)
        queues5 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues5)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_queue_get_marker_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(marker=queues[1], limit=1)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[2:3], queues2)
        filters = dict(marker=queues[0], limit=2)
        queues3 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[1:3], queues3)
        self.assertEquals([], list(self.backend.delete_queues('a')))
