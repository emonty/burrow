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

'''Memory backend for burrow.'''

import time

import burrow.backend


class Backend(burrow.backend.Backend):
    '''This backend stores all data using native Python data
    structures. It uses a linked list of tuples to store data
    (accounts, queues, and messages) with a dict as a secondary index
    into this list. This is required so we can have O(1) appends,
    deletes, and lookups by name, along with easy traversal starting
    anywhere in the list.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.accounts = Accounts()

    def delete_accounts(self, filters={}):
        self.accounts.delete(filters)

    def get_accounts(self, filters={}):
        for account in self.accounts.iter(filters):
            yield account[0]

    def delete_queues(self, account, filters={}):
        self.accounts.delete_queues(account, filters)

    def get_queues(self, account, filters={}):
        for queue in self.accounts.iter_queues(account, filters):
            yield queue[0]

    def delete_messages(self, account, queue, filters={}):
        return self._scan_queue(account, queue, filters, delete=True)

    def get_messages(self, account, queue, filters={}):
        return self._scan_queue(account, queue, filters)

    def update_messages(self, account, queue, attributes={}, filters={}):
        return self._scan_queue(account, queue, filters, attributes)

    def create_message(self, account, queue, message, body, attributes={}):
        account, queue = self.accounts.get_queue(account, queue, True)
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        for index in xrange(0, len(queue[3])):
            if queue[3][index]['id'] == message:
                message = queue[3][index]
                message['ttl'] = ttl
                message['hide'] = hide
                message['body'] = body
                if hide == 0:
                    self.notify(account[0], queue[0])
                return False
        message = dict(id=message, ttl=ttl, hide=hide, body=body)
        queue[3].append(message)
        self.notify(account[0], queue[0])
        return True

    def delete_message(self, account, queue, message):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        for index in xrange(0, len(queue[3])):
            if queue[3][index]['id'] == message:
                message = queue[3][index]
                del queue[3][index]
                if len(queue[3]) == 0:
                    self.accounts.delete_queue(account[0], queue[0])
                return message
        return None

    def get_message(self, account, queue, message):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        for index in xrange(0, len(queue[3])):
            if queue[3][index]['id'] == message:
                return queue[3][index]
        return None

    def update_message(self, account, queue, message, attributes={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        for index in xrange(0, len(queue[3])):
            if queue[3][index]['id'] == message:
                message = queue[3][index]
                if ttl is not None:
                    message['ttl'] = ttl
                if hide is not None:
                    message['hide'] = hide
                    if hide == 0:
                        self.notify(account[0], queue[0])
                return message
        return None

    def clean(self):
        now = int(time.time())
        for account in self.accounts.iter():
            for queue in account[3].iter():
                index = 0
                notify = False
                total = len(queue[3])
                while index < total:
                    message = queue[3][index]
                    if 0 < message['ttl'] <= now:
                        del queue[3][index]
                        total -= 1
                    else:
                        if 0 < message['hide'] <= now:
                            message['hide'] = 0
                            notify = True
                        index += 1
                if notify:
                    self.notify(account[0], queue[0])
                if len(queue[3]) == 0:
                    self.accounts.delete_queue(account[0], queue[0])

    def _scan_queue(self, account, queue, filters, attributes={},
        delete=False):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return
        index = 0
        notify = False
        if 'marker' in filters and filters['marker'] is not None:
            found = False
            for index in xrange(0, len(queue[3])):
                message = queue[3][index]
                if message['id'] == filters['marker']:
                    index += 1
                    found = True
                    break
            if not found:
                index = 0
        messages = []
        total = len(queue[3])
        limit = filters.get('limit', None)
        match_hidden = filters.get('match_hidden', False)
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        while index < total:
            message = queue[3][index]
            if not match_hidden and message['hide'] != 0:
                index += 1
                continue
            if ttl is not None:
                message['ttl'] = ttl
            if hide is not None:
                message['hide'] = hide
                if hide == 0:
                    notify = True
            if delete:
                del queue[3][index]
                total -= 1
            else:
                index += 1
            yield message
            if limit:
                limit -= 1
                if limit == 0:
                    break
        if notify:
            self.notify(account[0], queue[0])
        if len(queue[3]) == 0:
            self.accounts.delete_queue(account[0], queue[0])


class IndexedTupleList(object):
    '''Class for managing an indexed tuple list. The tuple must be at
    least three elements and must reserve the first three for (name,
    next, previous).'''

    def __init__(self):
        self.first = None
        self.last = None
        self.index = {}

    def add(self, item):
        item = (item[0], None, self.last) + item[3:]
        if self.first is None:
            self.first = item
        self.last = item
        self.index[item[0]] = item
        return item

    def count(self):
        return len(self.index)

    def delete(self, filters):
        if len(filters) == 0:
            self.first = None
            self.last = None
            self.index.clear()
            return
        for item in self.iter(filters):
            self.delete_item(item[0])

    def delete_item(self, name):
        if name not in self.index:
            return
        item = self.index[name][1]
        if item is not None:
            prev_item = self.index[name][2]
            self.index[item[0]] = (item[0], item[1], prev_item) + item[3:]
        item = self.index[name][2]
        if item is not None:
            next_item = self.index[name][1]
            self.index[item[0]] = (item[0], next_item, item[2]) + item[3:]
        if self.first == self.index[name]:
            self.first = self.index[name][1]
        if self.last == self.index[name]:
            self.last = self.index[name][2]
        del self.index[name]

    def get(self, name):
        if name in self.index:
            return self.index[name]
        return None

    def iter(self, filters={}):
        marker = filters.get('marker', None)
        if marker is not None and marker in self.index:
            item = self.index[marker]
        else:
            item = self.first
        limit = filters.get('limit', None)
        while item is not None:
            yield item
            if limit:
                limit -= 1
                if limit == 0:
                    break
            item = item[1]


class Accounts(IndexedTupleList):

    def delete_queue(self, account, queue):
        account = self.get(account)
        if account is not None:
            account[3].delete_item(queue)
            if account[3].count() == 0:
                self.delete_item(account[0])

    def delete_queues(self, account, filters):
        account = self.get(account)
        if account is not None:
            account[3].delete(filters)
            if account[3].count() == 0:
                self.delete_item(account[0])

    def get_queue(self, account, queue, create=False):
        if account in self.index:
            account = self.index[account]
        elif create:
            account = self.add((account, None, None, Queues()))
        else:
            return None, None
        return account, account[3].get(queue, create)

    def iter_queues(self, account, filters={}):
        account = self.get(account)
        if account is not None:
            for queue in account[3].iter(filters):
                yield queue


class Queues(IndexedTupleList):

    def get(self, queue, create=False):
        if queue in self.index:
            return self.index[queue]
        elif create:
            return self.add((queue, None, None, []))
        return None
