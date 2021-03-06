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

'''Unittests for burrow backends.'''

import eventlet
eventlet.monkey_patch(socket=True)

import testtools
import time

import burrow.backend


class Base(testtools.TestCase):
    '''Base test case.'''

    def __init__(self, *args, **kwargs):
        super(Base, self).__init__(*args, **kwargs)
        self.backend = None
        self.success = False

    def setUp(self):
        super(Base, self).setUp()
        self.addCleanup(self.check_empty)

    def check_empty(self):
        '''Ensure the backend is empty before, used before and after
        each test.'''
        accounts = self.backend.get_accounts()
        self.assertRaises(burrow.NotFound, list, accounts)
        queues = self.backend.get_queues('a')
        self.assertRaises(burrow.NotFound, list, queues)
        filters = dict(match_hidden=True)
        messages = self.backend.get_messages('a', 'q', filters)
        self.assertRaises(burrow.NotFound, list, messages)

    def delete_messages(self):
        '''Delete messages, including those that are hidden. Use
        after tests that don't need to delete.'''
        filters = dict(match_hidden=True)
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals([], messages)

    def get_messages(self):
        '''Get messages and ensure it is correct. Used for concurrent
        tests as an eventlet thread.'''
        message = dict(id='m', ttl=0, hide=0, body='test')
        filters = dict(wait=2)
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals([message], messages)
        self.success = True


class TestAccounts(Base):
    '''Test case for accounts.'''

    def test_basic(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['a'], list(self.backend.get_accounts()))
        self.assertEquals([], list(self.backend.delete_accounts()))
        accounts = self.backend.delete_accounts()
        self.assertRaises(burrow.NotFound, list, accounts)

    def test_large(self):
        for name in xrange(0, 1000):
            name = str(name)
            self.backend.create_message(name, name, name, name)
        filters = dict(marker='unknown')
        self.assertEquals([], list(self.backend.delete_accounts(filters)))

    def test_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals([dict(id='a')], accounts)

    def test_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals(['a'], accounts)

    def test_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        accounts = list(self.backend.delete_accounts(filters))
        self.assertEquals([], accounts)

    def test_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        accounts = self.backend.delete_accounts(filters)
        self.assertRaises(burrow.InvalidArguments, list, accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_delete_marker(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', marker=accounts[0])
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:], accounts2)
        accounts2 = list(self.backend.get_accounts())
        self.assertEquals(accounts[:1], accounts2)
        filters = dict(detail='id', marker='unknown')
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[:1], accounts2)

    def test_delete_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', limit=1)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[:1], accounts2)
        filters = dict(detail='id', limit=2)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:3], accounts2)

    def test_delete_marker_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(detail='id', marker=accounts[1], limit=1)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[2:3], accounts2)
        filters = dict(detail='id', marker=accounts[0], limit=2)
        accounts2 = list(self.backend.delete_accounts(filters))
        self.assertEquals(accounts[1:2], accounts2)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals([dict(id='a')], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals(['a'], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        accounts = list(self.backend.get_accounts(filters))
        self.assertEquals([], accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        accounts = self.backend.get_accounts(filters)
        self.assertRaises(burrow.InvalidArguments, list, accounts)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_marker(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(marker=accounts[0])
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[1:], accounts2)
        filters = dict(marker=accounts[1])
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[2:], accounts2)
        filters = dict(marker=accounts[2])
        accounts2 = self.backend.get_accounts(filters)
        self.assertRaises(burrow.NotFound, list, accounts2)
        filters = dict(marker='unknown')
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts2)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(limit=1)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[:1], accounts2)
        filters = dict(limit=2)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[:2], accounts2)
        filters = dict(limit=3)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts2)
        filters = dict(limit=100)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts, accounts2)
        self.assertEquals([], list(self.backend.delete_accounts()))

    def test_get_marker_limit(self):
        self.backend.create_message('a1', 'q', 'm', 'test')
        self.backend.create_message('a2', 'q', 'm', 'test')
        self.backend.create_message('a3', 'q', 'm', 'test')
        accounts = list(self.backend.get_accounts())
        self.assertEquals(3, len(accounts))
        filters = dict(marker=accounts[1], limit=1)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[2:3], accounts2)
        filters = dict(marker=accounts[0], limit=2)
        accounts2 = list(self.backend.get_accounts(filters))
        self.assertEquals(accounts[1:3], accounts2)
        self.assertEquals([], list(self.backend.delete_accounts()))


class TestQueues(Base):
    '''Test case for queues.'''

    def test_basic(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        self.assertEquals(['q'], list(self.backend.get_queues('a')))
        self.assertEquals([], list(self.backend.delete_queues('a')))
        queues = self.backend.delete_queues('a')
        self.assertRaises(burrow.NotFound, list, queues)

    def test_large(self):
        for name in xrange(0, 1000):
            name = str(name)
            self.backend.create_message('a', name, name, name)
        filters = dict(marker='unknown')
        self.assertEquals([], list(self.backend.delete_queues('a', filters)))

    def test_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals([dict(id='q')], queues)

    def test_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals(['q'], queues)

    def test_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        queues = list(self.backend.delete_queues('a', filters))
        self.assertEquals([], queues)

    def test_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        queues = self.backend.delete_queues('a', filters)
        self.assertRaises(burrow.InvalidArguments, list, queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_delete_marker(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', marker=queues[0])
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:], queues2)
        queues2 = list(self.backend.get_queues('a'))
        self.assertEquals(queues[:1], queues2)
        filters = dict(detail='id', marker='unknown')
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[:1], queues2)

    def test_delete_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', limit=1)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[:1], queues2)
        filters = dict(detail='id', limit=2)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:3], queues2)

    def test_delete_marker_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(detail='id', marker=queues[1], limit=1)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[2:3], queues2)
        filters = dict(detail='id', marker=queues[0], limit=2)
        queues2 = list(self.backend.delete_queues('a', filters))
        self.assertEquals(queues[1:2], queues2)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals([dict(id='q')], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals(['q'], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        queues = list(self.backend.get_queues('a', filters))
        self.assertEquals([], queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        queues = self.backend.get_queues('a', filters)
        self.assertRaises(burrow.InvalidArguments, list, queues)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_marker(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(marker=queues[0])
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[1:], queues2)
        filters = dict(marker=queues[1])
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[2:], queues2)
        filters = dict(marker=queues[2])
        queues2 = self.backend.get_queues('a', filters)
        self.assertRaises(burrow.NotFound, list, queues2)
        filters = dict(marker='unknown')
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues2)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(limit=1)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[:1], queues2)
        filters = dict(limit=2)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[:2], queues2)
        filters = dict(limit=3)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues2)
        filters = dict(limit=100)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues, queues2)
        self.assertEquals([], list(self.backend.delete_queues('a')))

    def test_get_marker_limit(self):
        self.backend.create_message('a', 'q1', 'm', 'test')
        self.backend.create_message('a', 'q2', 'm', 'test')
        self.backend.create_message('a', 'q3', 'm', 'test')
        queues = list(self.backend.get_queues('a'))
        self.assertEquals(3, len(queues))
        filters = dict(marker=queues[1], limit=1)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[2:3], queues2)
        filters = dict(marker=queues[0], limit=2)
        queues2 = list(self.backend.get_queues('a', filters))
        self.assertEquals(queues[1:3], queues2)
        self.assertEquals([], list(self.backend.delete_queues('a')))


class TestMessages(Base):
    '''Test case for messages.'''

    def test_basic(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=0, hide=0, body='test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals([message], messages)
        attributes = dict(ttl=100, hide=200)
        messages = list(self.backend.update_messages('a', 'q', attributes))
        self.assertEquals([], messages)
        attributes = dict(ttl=0, hide=0)
        filters = dict(match_hidden=True)
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals([], list(messages))
        messages = self.backend.update_messages('a', 'q', dict(), filters)
        self.assertEquals([], list(messages))
        self.delete_messages()
        messages = self.backend.delete_messages('a', 'q')
        self.assertRaises(burrow.NotFound, list, messages)
        messages = self.backend.update_messages('a', 'q', attributes)
        self.assertRaises(burrow.NotFound, list, messages)

    def test_large(self):
        for name in xrange(0, 1000):
            name = str(name)
            self.backend.create_message('a', 'q', name, name)
        attributes = dict(ttl=100, hide=200)
        messages = self.backend.update_messages('a', 'q', attributes)
        self.assertEquals([], list(messages))
        self.delete_messages()

    def test_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=0, hide=0, body='test')
        filters = dict(detail='all')
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals([message], messages)

    def test_delete_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=0, hide=0)
        filters = dict(detail='attributes')
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals([message], messages)

    def test_delete_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='body')
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(['test'], messages)

    def test_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(['m'], messages)

    def test_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        messages = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals([], messages)

    def test_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        messages = self.backend.delete_messages('a', 'q', filters)
        self.assertRaises(burrow.InvalidArguments, list, messages)
        self.assertEquals([], list(self.backend.delete_messages('a', 'q')))

    def test_delete_marker(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(detail='all', marker=messages[0]['id'])
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[1:], messages2)
        messages2 = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(messages[:1], messages2)
        filters = dict(detail='all', marker='unknown')
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[:1], messages2)

    def test_delete_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(detail='all', limit=1)
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[:1], messages2)
        filters = dict(detail='all', limit=2)
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[1:3], messages2)

    def test_delete_marker_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(detail='all', marker=messages[1]['id'], limit=1)
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[2:3], messages2)
        filters = dict(detail='all', marker=messages[0]['id'], limit=2)
        messages2 = list(self.backend.delete_messages('a', 'q', filters))
        self.assertEquals(messages[1:2], messages2)
        self.assertEquals([], list(self.backend.delete_messages('a', 'q')))

    def test_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=0, hide=0, body='test')
        filters = dict(detail='all')
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals([message], messages)
        self.delete_messages()

    def test_get_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=0, hide=0)
        filters = dict(detail='attributes')
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals([message], messages)
        self.delete_messages()

    def test_get_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='body')
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(['test'], messages)
        self.delete_messages()

    def test_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(['m'], messages)
        self.delete_messages()

    def test_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        messages = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals([], messages)
        self.delete_messages()

    def test_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        messages = self.backend.get_messages('a', 'q', filters)
        self.assertRaises(burrow.InvalidArguments, list, messages)
        self.delete_messages()

    def test_get_marker(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(marker=messages[0]['id'])
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[1:], messages2)
        filters = dict(marker=messages[1]['id'])
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[2:], messages2)
        filters = dict(marker=messages[2]['id'])
        messages2 = self.backend.get_messages('a', 'q', filters)
        self.assertRaises(burrow.NotFound, list, messages2)
        filters = dict(marker='unknown')
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages, messages2)
        self.delete_messages()

    def test_get_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(limit=1)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[:1], messages2)
        filters = dict(limit=2)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[:2], messages2)
        filters = dict(limit=3)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages, messages2)
        filters = dict(limit=100)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages, messages2)
        self.delete_messages()

    def test_get_marker_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        messages = list(self.backend.get_messages('a', 'q'))
        self.assertEquals(3, len(messages))
        filters = dict(marker=messages[1]['id'], limit=1)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[2:3], messages2)
        filters = dict(marker=messages[0]['id'], limit=2)
        messages2 = list(self.backend.get_messages('a', 'q', filters))
        self.assertEquals(messages[1:3], messages2)
        self.delete_messages()

    def test_update_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=100, hide=200, body='test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='all')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals([message], list(messages))
        self.delete_messages()

    def test_update_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = dict(id='m', ttl=100, hide=200)
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='attributes')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals([message], list(messages))
        self.delete_messages()

    def test_update_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='body')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(['test'], list(messages))
        self.delete_messages()

    def test_update_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='id')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(['m'], list(messages))
        self.delete_messages()

    def test_update_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='none')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals([], list(messages))
        self.delete_messages()

    def test_update_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='bad')
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertRaises(burrow.InvalidArguments, list, messages)
        self.delete_messages()

    def test_update_marker(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='all', match_hidden=True)
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        messages = list(messages)
        self.assertEquals(3, len(messages))
        filters.update(marker=messages[0]['id'])
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[1:], list(messages2))
        filters.update(marker=messages[1]['id'])
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[2:], list(messages2))
        filters.update(marker=messages[2]['id'])
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertRaises(burrow.NotFound, list, messages2)
        filters = dict(detail='all', marker='unknown', match_hidden=True)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages, list(messages2))
        self.delete_messages()

    def test_update_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='all', match_hidden=True)
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        messages = list(messages)
        self.assertEquals(3, len(messages))
        filters.update(limit=1)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[:1], list(messages2))
        filters.update(limit=2)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[:2], list(messages2))
        filters.update(limit=3)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages, list(messages2))
        filters.update(limit=100)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages, list(messages2))
        self.delete_messages()

    def test_update_marker_limit(self):
        self.backend.create_message('a', 'q', 'm1', 'test')
        self.backend.create_message('a', 'q', 'm2', 'test')
        self.backend.create_message('a', 'q', 'm3', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='all', match_hidden=True)
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        messages = list(messages)
        self.assertEquals(3, len(messages))
        filters.update(marker=messages[1]['id'], limit=1)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[2:3], list(messages2))
        filters.update(marker=messages[0]['id'], limit=2)
        messages2 = self.backend.update_messages('a', 'q', attributes, filters)
        self.assertEquals(messages[1:3], list(messages2))
        self.delete_messages()

    def test_update_wait(self):
        attributes = dict(hide=100)
        self.backend.create_message('a', 'q', 'm', 'test', attributes)
        self.success = False
        thread = eventlet.spawn(self.get_messages)
        attributes = dict(hide=0)
        filters = dict(match_hidden=True)
        messages = self.backend.update_messages('a', 'q', attributes, filters)
        eventlet.spawn_after(0.2, list, messages)
        thread.wait()
        self.assertTrue(self.success)
        self.delete_messages()


class TestMessage(Base):
    '''Test case for message.'''

    def test_basic(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        message = self.backend.get_message('a', 'q', 'm')
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test'), message)
        attributes = dict(ttl=100, hide=200)
        message = self.backend.update_message('a', 'q', 'm', attributes)
        attributes = dict(ttl=0, hide=0)
        message = self.backend.update_message('a', 'q', 'm', attributes)
        self.assertEquals(None, message)
        message = self.backend.update_message('a', 'q', 'm', dict())
        self.assertEquals(None, message)
        message = self.backend.delete_message('a', 'q', 'm')
        self.assertEquals(None, message)

    def test_create(self):
        created = self.backend.create_message('a', 'q', 'm', 'test1')
        self.assertEquals(created, True)
        message = self.backend.get_message('a', 'q', 'm')
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test1'), message)
        attributes = dict(ttl=100, hide=200)
        created = self.backend.create_message('a', 'q', 'm', 'test2',
            attributes)
        self.assertEquals(created, False)
        message = self.backend.get_message('a', 'q', 'm')
        self.assertEquals(dict(id='m', ttl=100, hide=200, body='test2'),
            message)
        attributes = dict(ttl=0, hide=0)
        created = self.backend.create_message('a', 'q', 'm', 'test3',
            attributes)
        self.assertEquals(created, False)
        message = self.backend.get_message('a', 'q', 'm')
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test3'), message)
        self.delete_messages()

    def test_delete_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        message = self.backend.delete_message('a', 'q', 'm', filters)
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test'), message)

    def test_delete_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='attributes')
        message = self.backend.delete_message('a', 'q', 'm', filters)
        self.assertEquals(dict(id='m', ttl=0, hide=0), message)

    def test_delete_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='body')
        message = self.backend.delete_message('a', 'q', 'm', filters)
        self.assertEquals('test', message)

    def test_delete_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        message = self.backend.delete_message('a', 'q', 'm', filters)
        self.assertEquals('m', message)

    def test_delete_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        message = self.backend.delete_message('a', 'q', 'm', filters)
        self.assertEquals(None, message)

    def test_delete_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        self.assertRaises(burrow.InvalidArguments,
            self.backend.delete_message, 'a', 'q', 'm', filters)
        self.delete_messages()

    def test_get_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='all')
        message = self.backend.get_message('a', 'q', 'm', filters)
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test'), message)
        self.delete_messages()

    def test_get_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='attributes')
        message = self.backend.get_message('a', 'q', 'm', filters)
        self.assertEquals(dict(id='m', ttl=0, hide=0), message)
        self.delete_messages()

    def test_get_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='body')
        message = self.backend.get_message('a', 'q', 'm', filters)
        self.assertEquals('test', message)
        self.delete_messages()

    def test_get_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='id')
        message = self.backend.get_message('a', 'q', 'm', filters)
        self.assertEquals('m', message)
        self.delete_messages()

    def test_get_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='none')
        message = self.backend.get_message('a', 'q', 'm', filters)
        self.assertEquals(None, message)
        self.delete_messages()

    def test_get_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        filters = dict(detail='bad')
        self.assertRaises(burrow.InvalidArguments,
            self.backend.get_message, 'a', 'q', 'm', filters)
        self.delete_messages()

    def test_update_detail_all(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='all')
        message = self.backend.update_message('a', 'q', 'm', attributes,
            filters)
        self.assertEquals(dict(id='m', ttl=100, hide=200, body='test'),
            message)
        self.delete_messages()

    def test_update_detail_attributes(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='attributes')
        message = self.backend.update_message('a', 'q', 'm', attributes,
            filters)
        self.assertEquals(dict(id='m', ttl=100, hide=200), message)
        self.delete_messages()

    def test_update_detail_body(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='body')
        message = self.backend.update_message('a', 'q', 'm', attributes,
            filters)
        self.assertEquals('test', message)
        self.delete_messages()

    def test_update_detail_id(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='id')
        message = self.backend.update_message('a', 'q', 'm', attributes,
            filters)
        self.assertEquals('m', message)
        self.delete_messages()

    def test_update_detail_none(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='none')
        message = self.backend.update_message('a', 'q', 'm', attributes,
            filters)
        self.assertEquals(None, message)
        self.delete_messages()

    def test_update_detail_bad(self):
        self.backend.create_message('a', 'q', 'm', 'test')
        attributes = dict(ttl=100, hide=200)
        filters = dict(detail='bad')
        self.assertRaises(burrow.InvalidArguments,
            self.backend.update_message, 'a', 'q', 'm', attributes, filters)
        self.delete_messages()

    def test_ttl(self):
        attributes = dict(ttl=1)
        self.backend.create_message('a', 'q', 'm', 'test', attributes)
        time.sleep(2)
        self.backend.clean()

    def test_ttl_large(self):
        attributes = dict(ttl=1)
        for name in xrange(0, 1000):
            name = str(name)
            self.backend.create_message('a', 'q', name, name, attributes)
        time.sleep(2)
        self.backend.clean()

    def test_hide(self):
        attributes = dict(hide=1)
        self.backend.create_message('a', 'q', 'm', 'test', attributes)
        time.sleep(2)
        self.backend.clean()
        message = self.backend.get_message('a', 'q', 'm')
        self.assertEquals(dict(id='m', ttl=0, hide=0, body='test'), message)
        self.delete_messages()

    def test_hide_large(self):
        attributes = dict(hide=1)
        for name in xrange(0, 1000):
            name = str(name)
            self.backend.create_message('a', 'q', name, name, attributes)
        time.sleep(2)
        self.backend.clean()
        message = self.backend.get_message('a', 'q', '0')
        self.assertEquals(dict(id='0', ttl=0, hide=0, body='0'), message)
        self.delete_messages()

    def test_create_wait(self):
        self.success = False
        thread = eventlet.spawn(self.get_messages)
        eventlet.spawn_after(0.2,
            self.backend.create_message, 'a', 'q', 'm', 'test')
        thread.wait()
        self.assertTrue(self.success)
        self.delete_messages()

    def test_update_wait(self):
        attributes = dict(hide=100)
        self.backend.create_message('a', 'q', 'm', 'test', attributes)
        self.success = False
        thread = eventlet.spawn(self.get_messages)
        attributes = dict(hide=0)
        eventlet.spawn_after(0.2,
            self.backend.update_message, 'a', 'q', 'm', attributes)
        thread.wait()
        self.assertTrue(self.success)
        self.delete_messages()
