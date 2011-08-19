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

'''Backends for burrow.'''

import time

import eventlet

import burrow.common


class Backend(burrow.common.Module):
    '''Interface that backend modules must implement.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.queues = {}

    def run(self, thread_pool):
        '''Run the backend. This should start any periodic tasks in
        separate threads and should never block.'''
        thread_pool.spawn_n(self._clean)

    def delete_accounts(self, filters=None):
        '''Delete accounts, which includes all queues and messages
        for the accounts. With no filters, this will delete all data
        for the entire server, so it should be used with caution.

        :param filters: Optional dict of filters for the request. Valid
        filters are 'marker', 'limit', and 'detail'. Valid values
        for 'detail' are 'none', 'id', and 'all'. Default value for
        'detail' is 'none'.
        :returns: Generator which will loop through all messages if
        'detail' is not 'none'.
        '''
        return []

    def get_accounts(self, filters=None):
        return []

    def delete_queues(self, account, filters=None):
        return []

    def get_queues(self, account, filters=None):
        return []

    def delete_messages(self, account, queue, filters=None):
        return []

    def get_messages(self, account, queue, filters=None):
        return []

    def update_messages(self, account, queue, attributes, filters=None):
        return []

    def create_message(self, account, queue, message, body, attributes=None):
        return True

    def delete_message(self, account, queue, message, filters=None):
        return None

    def get_message(self, account, queue, message, filters=None):
        return None

    def update_message(self, account, queue, message, attributes,
        filters=None):
        return None

    def clean(self):
        '''This method should remove all messages with an expired
        TTL and make hidden messages that have an expired hide time
        visible again.'''
        pass

    def _clean(self):
        '''Thread to run the clean method periodically.'''
        while True:
            self.clean()
            eventlet.sleep(1)

    def _get_attributes(self, attributes, ttl=None, hide=None):
        if attributes is not None:
            ttl = attributes.get('ttl', ttl)
            hide = attributes.get('hide', hide)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        if hide is not None and hide > 0:
            hide += int(time.time())
        return ttl, hide

    def _get_detail(self, filters, default=None):
        detail = default if filters is None else filters.get('detail', default)
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.backend.InvalidArguments(detail)
        return detail

    def _get_message_detail(self, filters, default=None):
        detail = default if filters is None else filters.get('detail', default)
        options = ['id', 'attributes', 'body', 'all']
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in options:
            raise burrow.backend.InvalidArguments(detail)
        return detail

    def notify(self, account, queue):
        '''Notify any waiting callers that the account/queue has
        a visible message.'''
        queue = '%s/%s' % (account, queue)
        if queue in self.queues:
            for count in xrange(0, self.queues[queue].getting()):
                self.queues[queue].put(0)

    def wait(self, account, queue, seconds):
        '''Wait for a message to appear in the account/queue.'''
        queue = '%s/%s' % (account, queue)
        if queue not in self.queues:
            self.queues[queue] = eventlet.Queue()
        try:
            self.queues[queue].get(timeout=seconds)
        except eventlet.Queue.Empty:
            pass
        if self.queues[queue].getting() == 0:
            del self.queues[queue]


def wait_without_attributes(method):
    def wrapper(self, account, queue, filters=None):
        original = lambda: method(self, account, queue, filters)
        return wait(self, account, queue, filters, original)
    return wrapper


def wait_with_attributes(method):
    def wrapper(self, account, queue, attributes, filters=None):
        original = lambda: method(self, account, queue, attributes, filters)
        return wait(self, account, queue, filters, original)
    return wrapper


def wait(self, account, queue, filters, method):
    '''Decorator to wait on a queue if the wait option is given. This
    will block until a message in the queue is ready or the timeout
    expires.'''
    seconds = 0 if filters is None else filters.get('wait', 0)
    if seconds > 0:
        seconds += time.time()
    while True:
        try:
            for message in method():
                yield message
            return
        except burrow.backend.NotFound, exception:
            now = time.time()
            if seconds - now > 0:
                self.wait(account, queue, seconds - now)
            if seconds < time.time():
                raise exception


class NotFound(Exception):
    pass


class InvalidArguments(Exception):
    pass
