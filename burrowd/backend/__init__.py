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

'''Backends for the burrow server.'''

import eventlet

import burrowd


class Backend(burrowd.Module):
    '''Interface that backend implementations must provide.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.queues = {}

    def run(self, thread_pool):
        '''Run the backend. This should start any periodic tasks in
        separate threads and should never block.'''
        thread_pool.spawn_n(self._clean)

    def delete_accounts(self, filters={}):
        pass

    def get_accounts(self, filters={}):
        return []

    def delete_queues(self, account, filters={}):
        pass

    def get_queues(self, account, filters={}):
        return []

    def delete_messages(self, account, queue, filters={}):
        return []

    def get_messages(self, account, queue, filters={}):
        return []

    def update_messages(self, account, queue, attributes={}, filters={}):
        return []

    def create_message(self, account, queue, message, body, attributes={}):
        return True

    def delete_message(self, account, queue, message):
        return None

    def get_message(self, account, queue, message):
        return None

    def update_message(self, account, queue, message, attributes={}):
        return None

    def notify(self, account, queue):
        '''Notify any waiting callers that the account/queue has
        a visible message.'''
        queue = '%s/%s' % (account, queue)
        if queue in self.queues:
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
