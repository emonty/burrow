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
    deletes, and lookups by id, along with easy traversal starting
    anywhere in the list.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.accounts = Accounts()

    def delete_accounts(self, filters={}):
        if len(filters) == 0:
            self.accounts.reset()
            return
        detail = self._get_detail(filters)
        for account in self.accounts.iter(filters):
            self.accounts.delete(account.id)
            if detail is not None:
                yield account.detail(detail)

    def get_accounts(self, filters={}):
        detail = self._get_detail(filters, 'id')
        for account in self.accounts.iter(filters):
            if detail is not None:
                yield account.detail(detail)

    def delete_queues(self, account, filters={}):
        account = self.accounts.get(account)
        if account is None:
            raise burrow.backend.NotFound()
        if len(filters) == 0:
            account.queues.reset()
        else:
            detail = self._get_detail(filters)
            for queue in account.queues.iter(filters):
                account.queues.delete(queue.id)
                if detail is not None:
                    yield queue.detail(detail)
        if account.queues.count() == 0:
            self.accounts.delete(account.id)

    def get_queues(self, account, filters={}):
        account = self.accounts.get(account)
        if account is None:
            raise burrow.backend.NotFound()
        detail = self._get_detail(filters, 'id')
        for queue in account.queues.iter(filters):
            if detail is not None:
                yield queue.detail(detail)

    def delete_messages(self, account, queue, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            raise burrow.backend.NotFound()
        if len(filters) == 0:
            queue.messages.reset()
        else:
            detail = self._get_message_detail(filters)
            for message in queue.messages.iter(filters):
                queue.messages.delete(message.id)
                if detail is not None:
                    yield message.detail(detail)
        if queue.messages.count() == 0:
            self.accounts.delete_queue(account.id, queue.id)

    def get_messages(self, account, queue, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            raise burrow.backend.NotFound()
        detail = self._get_message_detail(filters, 'all')
        for message in queue.messages.iter(filters):
            if detail is not None:
                yield message.detail(detail)

    def update_messages(self, account, queue, attributes={}, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            raise burrow.backend.NotFound()
        notify = False
        ttl, hide = self._get_attributes(attributes)
        detail = self._get_message_detail(filters)
        for message in queue.messages.iter(filters):
            if ttl is not None:
                message.ttl = ttl
            if hide is not None:
                message.hide = hide
                if hide == 0:
                    notify = True
            if detail is not None:
                yield message.detail(detail)
        if notify:
            self.notify(account.id, queue.id)

    def create_message(self, account, queue, message, body, attributes={}):
        account, queue = self.accounts.get_queue(account, queue, True)
        ttl, hide = self._get_attributes(attributes, default_ttl=0,
            default_hide=0)
        if queue.messages.get(message) is None:
            created = True
        else:
            created = False
        message = queue.messages.get(message, True)
        message.ttl = ttl
        message.hide = hide
        message.body = body
        if created or hide == 0:
            self.notify(account.id, queue.id)
        return created

    def delete_message(self, account, queue, message, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        message = queue.messages.get(message)
        if message is None:
            return None
        queue.messages.delete(message.id)
        if queue.messages.count() == 0:
            self.accounts.delete_queue(account.id, queue.id)
        return message.detail()

    def get_message(self, account, queue, message, filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        message = queue.messages.get(message)
        if message is None:
            return None
        return message.detail()

    def update_message(self, account, queue, message, attributes={},
        filters={}):
        account, queue = self.accounts.get_queue(account, queue)
        if queue is None:
            return None
        ttl, hide = self._get_attributes(attributes)
        message = queue.messages.get(message)
        if message is None:
            return None
        if ttl is not None:
            message.ttl = ttl
        if hide is not None:
            message.hide = hide
            if hide == 0:
                self.notify(account.id, queue.id)
        return message.detail()

    def clean(self):
        now = int(time.time())
        for account in self.accounts.iter():
            for queue in account.queues.iter():
                notify = False
                for message in queue.messages.iter(dict(match_hidden=True)):
                    if 0 < message.ttl <= now:
                        queue.messages.delete(message.id)
                    elif 0 < message.hide <= now:
                        message.hide = 0
                        notify = True
                if notify:
                    self.notify(account.id, queue.id)
                if queue.messages.count() == 0:
                    self.accounts.delete_queue(account.id, queue.id)

    def _get_attributes(self, attributes, default_ttl=None, default_hide=None):
        ttl = attributes.get('ttl', default_ttl)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', default_hide)
        if hide is not None and hide > 0:
            hide += int(time.time())
        return ttl, hide

    def _get_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.backend.BadDetail(detail)
        return detail

    def _get_message_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        options = ['id', 'attributes', 'body', 'all']
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in options:
            raise burrow.backend.BadDetail(detail)
        return detail


class Item(object):
    '''Object to represent elements in a indexed linked list.'''

    def __init__(self, id=None):
        self.id = id
        self.next = None
        self.prev = None

    def detail(self, detail):
        if detail == 'all':
            return dict(id=self.id)
        return self.id


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
        self.index[item.id] = item
        return item

    def count(self):
        return len(self.index)

    def delete(self, id):
        if id not in self.index:
            return
        item = self.index.pop(id)
        if item.next is not None:
            item.next.prev = item.prev
        if item.prev is not None:
            item.prev.next = item.next
        if self.first == item:
            self.first = item.next
        if self.last == item:
            self.last = item.prev

    def get(self, id, create=False):
        if id in self.index:
            return self.index[id]
        elif create:
            return self.add(self.item_class(id))
        return None

    def iter(self, filters={}):
        marker = filters.get('marker', None)
        if marker is not None and marker in self.index:
            item = self.index[marker].next
        else:
            item = self.first
        if item is None:
            raise burrow.backend.NotFound()
        limit = filters.get('limit', None)
        while item is not None:
            yield item
            if limit:
                limit -= 1
                if limit == 0:
                    break
            item = item.next

    def reset(self):
        if self.count() == 0:
            raise burrow.backend.NotFound()
        self.first = None
        self.last = None
        self.index.clear()


class Account(Item):

    def __init__(self, id=None):
        super(Account, self).__init__(id)
        self.queues = Queues()


class Accounts(IndexedList):

    item_class = Account

    def delete_queue(self, account, queue):
        account = self.get(account)
        if account is not None:
            account.queues.delete(queue)
            if account.queues.count() == 0:
                self.delete(account.id)

    def get_queue(self, account, queue, create=False):
        if account in self.index:
            account = self.index[account]
        elif create:
            account = self.add(Account(account))
        else:
            return None, None
        return account, account.queues.get(queue, create)


class Queue(Item):

    def __init__(self, id=None):
        super(Queue, self).__init__(id)
        self.messages = Messages()


class Queues(IndexedList):

    item_class = Queue


class Message(Item):

    def __init__(self, id=None):
        super(Message, self).__init__(id)
        self.ttl = 0
        self.hide = 0
        self.body = None

    def detail(self, detail=None):
        if detail == 'id':
            return self.id
        elif detail == 'body':
            return self.body
        ttl = self.ttl
        if ttl > 0:
            ttl -= int(time.time())
        hide = self.hide
        if hide > 0:
            hide -= int(time.time())
        if detail == 'attributes':
            return dict(id=self.id, ttl=ttl, hide=hide)
        return dict(id=self.id, ttl=ttl, hide=hide, body=self.body)


class Messages(IndexedList):

    item_class = Message

    def iter(self, filters={}):
        marker = filters.get('marker', None)
        if marker is not None and marker in self.index:
            item = self.index[marker].next
        else:
            item = self.first
        limit = filters.get('limit', None)
        match_hidden = filters.get('match_hidden', False)
        count = 0
        while item is not None:
            if match_hidden or item.hide == 0:
                count += 1
                yield item
                if limit:
                    limit -= 1
                    if limit == 0:
                        break
            item = item.next
        if count == 0:
            raise burrow.backend.NotFound()
