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
    structures. It uses a linked list of objects to store data
    (accounts, queues, and messages) with a dict as a secondary index
    into this list. This is required so we can have O(1) appends,
    deletes, and lookups by name, along with easy traversal starting
    anywhere in the list.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.accounts = Accounts()

    def delete_accounts(self, filters={}):
        return self.accounts.delete(filters)

    def get_accounts(self, filters={}):
        return self.accounts.iter_detail(filters)

    def delete_queues(self, account, filters={}):
        return self.accounts.delete_queues(account, filters)

    def get_queues(self, account, filters={}):
        return self.accounts.get_queues(account, filters)

    def delete_messages(self, account, queue, filters={}):
        return self._scan_queue(account, queue, filters, delete=True)

    def get_messages(self, account, queue, filters={}):
        return self._scan_queue(account, queue, filters)

    def update_messages(self, account, queue, attributes={}, filters={}):
        return self._scan_queue(account, queue, filters, attributes)

    def create_message(self, account, queue, message, body, attributes={}):
        account, queue = self.accounts.get_queue(account, queue, True)
        ttl = attributes.get('ttl', 0)
        if ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', 0)
        if hide > 0:
            hide += int(time.time())
        for index in xrange(0, len(queue.messages)):
            if queue.messages[index]['id'] == message:
                message = queue.messages[index]
                message['ttl'] = ttl
                message['hide'] = hide
                message['body'] = body
                if hide == 0:
                    self.notify(account.name, queue.name)
                return False
        message = dict(id=message, ttl=ttl, hide=hide, body=body)
        queue.messages.append(message)
        self.notify(account.name, queue.name)
        return True

    def delete_message(self, account, queue, message, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        for index in xrange(0, len(queue.messages)):
            if queue.messages[index]['id'] == message:
                message = queue.messages[index]
                del queue.messages[index]
                if len(queue.messages) == 0:
                    self.accounts.delete_queue(account.name, queue.name)
                if message['ttl'] > 0:
                    message['ttl'] -= int(time.time())
                if message['hide'] > 0:
                    message['hide'] -= int(time.time())
                return message
        return None

    def get_message(self, account, queue, message, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        for index in xrange(0, len(queue.messages)):
            if queue.messages[index]['id'] == message:
                ttl = queue.messages[index]['ttl']
                if ttl > 0:
                    ttl -= int(time.time())
                hide = queue.messages[index]['hide']
                if hide > 0:
                    hide -= int(time.time())
                return dict(id=message, ttl=ttl, hide=hide,
                    body=queue.messages[index]['body'])
        return None

    def update_message(self, account, queue, message, attributes={},
        filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        ttl = attributes.get('ttl', None)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', None)
        if hide is not None and hide > 0:
            hide += int(time.time())
        for index in xrange(0, len(queue.messages)):
            if queue.messages[index]['id'] == message:
                message = queue.messages[index]
                if ttl is not None:
                    message['ttl'] = ttl
                if hide is not None:
                    message['hide'] = hide
                    if hide == 0:
                        self.notify(account.name, queue.name)
                return message
        return None

    def clean(self):
        now = int(time.time())
        for account in self.accounts.iter():
            for queue in account.queues.iter():
                index = 0
                notify = False
                total = len(queue.messages)
                while index < total:
                    message = queue.messages[index]
                    if 0 < message['ttl'] <= now:
                        del queue.messages[index]
                        total -= 1
                    else:
                        if 0 < message['hide'] <= now:
                            message['hide'] = 0
                            notify = True
                        index += 1
                if notify:
                    self.notify(account.name, queue.name)
                if len(queue.messages) == 0:
                    self.accounts.delete_queue(account.name, queue.name)

    def _scan_queue(self, account, queue, filters, attributes={},
        delete=False):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return
        index = 0
        notify = False
        if 'marker' in filters and filters['marker'] is not None:
            found = False
            for index in xrange(0, len(queue.messages)):
                message = queue.messages[index]
                if message['id'] == filters['marker']:
                    index += 1
                    found = True
                    break
            if not found:
                index = 0
        messages = []
        total = len(queue.messages)
        limit = filters.get('limit', None)
        match_hidden = filters.get('match_hidden', False)
        ttl = attributes.get('ttl', None)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', None)
        if hide is not None and hide > 0:
            hide += int(time.time())
        while index < total:
            message = queue.messages[index]
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
                del queue.messages[index]
                total -= 1
            else:
                index += 1
            relative_ttl = message['ttl']
            if relative_ttl > 0:
                relative_ttl -= int(time.time())
            relative_hide = message['hide']
            if relative_hide > 0:
                relative_hide -= int(time.time())
            yield dict(id=message['id'], ttl=relative_ttl, hide=relative_hide,
                    body=message['body'])
            if limit:
                limit -= 1
                if limit == 0:
                    break
        if notify:
            self.notify(account.name, queue.name)
        if len(queue.messages) == 0:
            self.accounts.delete_queue(account.name, queue.name)


class Item(object):
    '''Object to represent elements in a indexed linked list.'''

    def __init__(self, name=None):
        self.name = name
        self.next = None
        self.prev = None


class IndexedList(object):
    '''Class for managing an indexed linked list.'''

    def __init__(self):
        self.first = None
        self.last = None
        self.index = {}

    def add(self, item):
        if self.first is None:
            self.first = item
        if self.last is not None:
            item.prev = self.last
            self.last.next = item
        self.last = item
        self.index[item.name] = item
        return item

    def count(self):
        return len(self.index)

    def delete(self, filters):
        if len(filters) == 0:
            self.first = None
            self.last = None
            self.index.clear()
            return
        detail = self._get_detail(filters)
        for item in self.iter(filters):
            self.delete_item(item.name)
            if detail is 'id':
                yield item.name
            elif detail is 'all':
                yield dict(id=item.name)

    def delete_item(self, name):
        if name not in self.index:
            return
        item = self.index.pop(name)
        if item.next is not None:
            item.next.prev = item.prev
        if item.prev is not None:
            item.prev.next = item.next
        if self.first == item:
            self.first = item.next
        if self.last == item:
            self.last = item.prev

    def get(self, name):
        if name in self.index:
            return self.index[name]
        return None

    def iter(self, filters={}):
        marker = filters.get('marker', None)
        if marker is not None and marker in self.index:
            item = self.index[marker].next
        else:
            item = self.first
        limit = filters.get('limit', None)
        while item is not None:
            yield item
            if limit:
                limit -= 1
                if limit == 0:
                    break
            item = item.next

    def iter_detail(self, filters={}):
        detail = self._get_detail(filters, 'id')
        for item in self.iter(filters):
            if detail is 'id':
                yield item.name
            elif detail is 'all':
                yield dict(id=item.name)

    def _get_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        if detail is 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.backend.BadDetail(detail)
        return detail


class Account(Item):

    def __init__(self, name=None):
        super(Account, self).__init__(name)
        self.queues = Queues()


class Accounts(IndexedList):

    def delete_queue(self, account, queue):
        account = self.get(account)
        if account is not None:
            account.queues.delete_item(queue)
            if account.queues.count() == 0:
                self.delete_item(account.name)

    def delete_queues(self, account, filters):
        account = self.get(account)
        if account is not None:
            for queue in account.queues.delete(filters):
                yield queue
            if account.queues.count() == 0:
                self.delete_item(account.name)

    def get_queue(self, account, queue, create=False):
        if account in self.index:
            account = self.index[account]
        elif create:
            account = self.add(Account(account))
        else:
            return None, None
        return account, account.queues.get(queue, create)

    def get_queues(self, account, filters={}):
        account = self.get(account)
        if account is None:
            return []
        else:
            return account.queues.iter_detail(filters)


class Queue(Item):

    def __init__(self, name=None):
        super(Queue, self).__init__(name)
        self.messages = []


class Queues(IndexedList):

    def get(self, queue, create=False):
        if queue in self.index:
            return self.index[queue]
        elif create:
            return self.add(Queue(queue))
        return None
