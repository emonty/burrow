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

'''Unittests for the Client API.'''

import testtools

import burrow


class TestClient(testtools.TestCase):
    '''Test case for Client API.'''

    def test_client(self):
        client = burrow.Client()
        self.assertRaises(burrow.NotFound, list, client.get_accounts())
        self.assertEquals(True, client.create_message('a', 'q', 'm', 'body'))
        self.assertEquals(['a'], list(client.get_accounts()))
        self.assertEquals([], list(client.delete_accounts()))

    def test_account(self):
        account = burrow.Account('a')
        self.assertRaises(burrow.NotFound, list, account.get_queues())
        self.assertEquals(True, account.create_message('q', 'm', 'body'))
        self.assertEquals(['q'], list(account.get_queues()))
        self.assertEquals([], list(account.delete_accounts()))

    def test_queue(self):
        queue = burrow.Queue('a', 'q')
        self.assertRaises(burrow.NotFound, list, queue.get_messages())
        self.assertEquals(True, queue.create_message('m', 'body'))
        messages = queue.get_messages(filters=dict(detail='id'))
        self.assertEquals(['m'], list(messages))
        self.assertEquals([], list(queue.delete_accounts()))

    def test_url(self):
        client = burrow.Client(url='http://localhost:8080')
        self.assertRaises(burrow.NotFound, list, client.get_accounts())
        self.assertEquals(True, client.create_message('a', 'q', 'm', 'body'))
        self.assertEquals(['a'], list(client.get_accounts()))
        self.assertEquals([], list(client.delete_accounts()))

    def test_shared_client(self):
        client = burrow.Client(url='http://localhost:8080')
        self.assertRaises(burrow.NotFound, list, client.get_accounts())
        self.assertEquals(True, client.create_message('a', 'q', 'm', 'body'))
        account = burrow.Account('a', client=client)
        self.assertEquals(['q'], list(account.get_queues()))
        self.assertEquals([], list(account.delete_accounts()))
