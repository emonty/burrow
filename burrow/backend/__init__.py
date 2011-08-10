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
    '''Interface that backend implementations must provide.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.queues = {}

    def run(self, thread_pool):
        '''Run the backend. This should start any periodic tasks in
        separate threads and should never block.'''
        thread_pool.spawn_n(self._clean)

    def delete_accounts(self, filters={}):
        return []

    def get_accounts(self, filters={}):
        return []

    def delete_queues(self, account, filters={}):
        return []

    def get_queues(self, account, filters={}):
        return []

    def delete_messages(self, account, queue, filters={}):
        return []

    def get_messages(self, account, queue, filters={}):
        return []

    def update_messages(self, account, queue, attributes, filters={}):
        return []

    def create_message(self, account, queue, message, body, attributes={}):
        return True

    def delete_message(self, account, queue, message, filters={}):
        return None

    def get_message(self, account, queue, message, filters={}):
        return None

    def update_message(self, account, queue, message, attributes, filters={}):
        return None

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
        except Exception:
            pass
        if self.queues[queue].getting() == 0:
            del self.queues[queue]

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
        ttl = attributes.get('ttl', ttl)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', hide)
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


class NotFound(Exception):
    pass


class BadDetail(Exception):
    pass
