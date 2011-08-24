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
    (accounts, queues, and messages) with a dictionary as a secondary
    index into this list. This is required so we can have O(1) appends,
    deletes, and lookups by id, along with easy traversal starting
    anywhere in the list.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.accounts = Accounts()

    def delete_accounts(self, filters=None):
        if filters is None or len(filters) == 0:
            self.accounts.reset()
            return
        detail = self._get_detail(filters)
        for account in self.accounts.iter(filters):
            self.accounts.delete(account.id)
            if detail is not None:
                yield account.detail(detail)

    def get_accounts(self, filters=None):
        detail = self._get_detail(filters, 'id')
        for account in self.accounts.iter(filters):
            if detail is not None:
                yield account.detail(detail)

    def delete_queues(self, account, filters=None):
        account = self.accounts.get(account)
        if filters is None or len(filters) == 0:
            account.queues.reset()
        else:
            detail = self._get_detail(filters)
            for queue in account.queues.iter(filters):
                account.queues.delete(queue.id)
                if detail is not None:
                    yield queue.detail(detail)
        if account.queues.count() == 0:
            self.accounts.delete(account.id)

    def get_queues(self, account, filters=None):
        account = self.accounts.get(account)
        detail = self._get_detail(filters, 'id')
        for queue in account.queues.iter(filters):
            if detail is not None:
                yield queue.detail(detail)

    @burrow.backend.wait_without_attributes
    def delete_messages(self, account, queue, filters=None):
        account, queue = self.accounts.get_queue(account, queue)
        detail = self._get_message_detail(filters)
        for message in queue.messages.iter(filters):
            queue.messages.delete(message.id)
            if detail is not None:
                yield message.detail(detail)
        if queue.messages.count() == 0:
            self.accounts.delete_queue(account.id, queue.id)

    @burrow.backend.wait_without_attributes
    def get_messages(self, account, queue, filters=None):
        account, queue = self.accounts.get_queue(account, queue)
        detail = self._get_message_detail(filters, 'all')
        for message in queue.messages.iter(filters):
            if detail is not None:
                yield message.detail(detail)

    @burrow.backend.wait_with_attributes
    def update_messages(self, account, queue, attributes, filters=None):
        account, queue = self.accounts.get_queue(account, queue)
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

    def create_message(self, account, queue, message, body, attributes=None):
        account, queue = self.accounts.get_queue(account, queue, True)
        ttl, hide = self._get_attributes(attributes, ttl=0, hide=0)
        try:
            message = queue.messages.get(message)
            created = False
        except burrow.NotFound:
            message = queue.messages.get(message, True)
            created = True
        message.ttl = ttl
        message.hide = hide
        message.body = body
        if created or hide == 0:
            self.notify(account.id, queue.id)
        return created

    def delete_message(self, account, queue, message, filters=None):
        account, queue = self.accounts.get_queue(account, queue)
        message = queue.messages.get(message)
        detail = self._get_message_detail(filters)
        queue.messages.delete(message.id)
        if queue.messages.count() == 0:
            self.accounts.delete_queue(account.id, queue.id)
        return message.detail(detail)

    def get_message(self, account, queue, message, filters=None):
        account, queue = self.accounts.get_queue(account, queue)
        message = queue.messages.get(message)
        detail = self._get_message_detail(filters, 'all')
        return message.detail(detail)

    def update_message(self, account, queue, message, attributes,
        filters=None):
        account, queue = self.accounts.get_queue(account, queue)
        message = queue.messages.get(message)
        ttl, hide = self._get_attributes(attributes)
        detail = self._get_message_detail(filters)
        if ttl is not None:
            message.ttl = ttl
        if hide is not None:
            message.hide = hide
            if hide == 0:
                self.notify(account.id, queue.id)
        return message.detail(detail)

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


class Item(object):
    '''Object to represent elements in a indexed linked list.'''

    def __init__(self, id=None):
        self.id = id
        self.next = None
        self.prev = None

    def detail(self, detail):
        '''Format detail response for this item.'''
        if detail == 'id':
            return self.id
        elif detail == 'all':
            return dict(id=self.id)
        return None


class IndexedList(object):
    '''Class for managing an indexed linked list.'''

    item_class = Item

    def __init__(self):
        self.first = None
        self.last = None
        self.index = {}

    def add(self, item):
        '''Add a new item to the list.'''
        if self.first is None:
            self.first = item
        if self.last is not None:
            item.prev = self.last
            self.last.next = item
        self.last = item
        self.index[item.id] = item
        return item

    def count(self):
        '''Return a count of the number of items in the list.'''
        return len(self.index)

    def delete(self, id):
        '''Delete an item from the list by id.'''
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
        '''Get an item from the list by id.'''
        if id in self.index:
            return self.index[id]
        elif create:
            return self.add(self.item_class(id))
        raise burrow.NotFound(self.item_class.__name__ + " not found")

    def iter(self, filters=None):
        '''Iterate through all items in the list, possibly filtered.'''
        if filters is None:
            marker = None
            limit = None
        else:
            marker = filters.get('marker', None)
            limit = filters.get('limit', None)
        if marker is not None and marker in self.index:
            item = self.index[marker].next
        else:
            item = self.first
        if item is None:
            raise burrow.NotFound(self.item_class.__name__ + " not found")
        while item is not None:
            yield item
            if limit:
                limit -= 1
                if limit == 0:
                    break
            item = item.next

    def reset(self):
        '''Remove all items in the list.'''
        if self.count() == 0:
            raise burrow.NotFound(self.item_class.__name__ + " not found")
        self.first = None
        self.last = None
        self.index.clear()


class Account(Item):
    '''A type of item representing an account.'''

    def __init__(self, id=None):
        super(Account, self).__init__(id)
        self.queues = Queues()


class Accounts(IndexedList):
    '''A type of list representing an account list.'''

    item_class = Account

    def delete_queue(self, account, queue):
        '''Delete a queue within the given account.'''
        account = self.get(account)
        if account is not None:
            account.queues.delete(queue)
            if account.queues.count() == 0:
                self.delete(account.id)

    def get_queue(self, account, queue, create=False):
        '''Get a queue within the given the account.'''
        if account in self.index:
            account = self.index[account]
        elif create:
            account = self.add(Account(account))
        else:
            raise burrow.NotFound('Account not found')
        return account, account.queues.get(queue, create)


class Queue(Item):
    '''A type of item representing a queue.'''

    def __init__(self, id=None):
        super(Queue, self).__init__(id)
        self.messages = Messages()


class Queues(IndexedList):
    '''A type of list representing a queue list.'''

    item_class = Queue


class Message(Item):
    '''A type of item representing a message.'''

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
        elif detail == 'all':
            return dict(id=self.id, ttl=ttl, hide=hide, body=self.body)
        return None


class Messages(IndexedList):
    '''A type of list representing a message list.'''

    item_class = Message

    def iter(self, filters=None):
        if filters is None:
            marker = None
            limit = None
            match_hidden = False
        else:
            marker = filters.get('marker', None)
            limit = filters.get('limit', None)
            match_hidden = filters.get('match_hidden', False)
        if marker is not None and marker in self.index:
            item = self.index[marker].next
        else:
            item = self.first
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
            raise burrow.NotFound('Message not found')
