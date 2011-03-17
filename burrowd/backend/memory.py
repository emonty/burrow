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

'''Memory backend for the burrow server.'''

import time

import burrowd.backend


class Backend(burrowd.backend.Backend):

    def __init__(self, config):
        super(Backend, self).__init__(config)
        self.accounts = {}

    def delete_accounts(self):
        self.accounts.clear()

    def get_accounts(self):
        return self.accounts.keys()

    def delete_account(self, account):
        del self.accounts[account]

    def get_queues(self, account):
        if account not in self.accounts:
            return []
        return self.accounts[account].keys()

    def queue_exists(self, account, queue):
        return account in self.accounts and queue in self.accounts[account]

    def delete_messages(self, account, queue, limit, marker, match_hidden):
        messages = self._scan_queue(account, queue, limit, marker,
            match_hidden, delete=True)
        if len(self.accounts[account][queue]) == 0:
            del self.accounts[account][queue]
        if len(self.accounts[account]) == 0:
            del self.accounts[account]
        return messages

    def get_messages(self, account, queue, limit, marker, match_hidden):
        return self._scan_queue(account, queue, limit, marker, match_hidden)

    def update_messages(self, account, queue, limit, marker, match_hidden, ttl,
                        hide):
        return self._scan_queue(account, queue, limit, marker, match_hidden,
                                ttl=ttl, hide=hide)

    def delete_message(self, account, queue, message_id):
        for index in range(0, len(self.accounts[account][queue])):
            message = self.accounts[account][queue][index]
            if message['id'] == message_id:
                del self.accounts[account][queue][index]
                if len(self.accounts[account][queue]) == 0:
                    del self.accounts[account][queue]
                if len(self.accounts[account]) == 0:
                    del self.accounts[account]
                return message
        return None

    def get_message(self, account, queue, message_id):
        for index in range(0, len(self.accounts[account][queue])):
            message = self.accounts[account][queue][index]
            if message['id'] == message_id:
                return message
        return None

    def put_message(self, account, queue, message_id, ttl, hide, body):
        if account not in self.accounts:
            self.accounts[account] = {}
        if queue not in self.accounts[account]:
            self.accounts[account][queue] = []
        for index in range(0, len(self.accounts[account][queue])):
            message = self.accounts[account][queue][index]
            if message['id'] == message_id:
                message['ttl'] = ttl
                message['hide'] = hide
                message['body'] = body
                if hide == 0:
                    self.notify(account, queue)
                return False
        message = dict(id=message_id, ttl=ttl, hide=hide, body=body)
        self.accounts[account][queue].append(message)
        self.notify(account, queue)
        return True

    def update_message(self, account, queue, message_id, ttl, hide):
        for index in range(0, len(self.accounts[account][queue])):
            message = self.accounts[account][queue][index]
            if message['id'] == message_id:
                if ttl is not None:
                    message['ttl'] = ttl
                if hide is not None:
                    message['hide'] = hide
                    if hide == 0:
                        self.notify(account, queue)
                return message
        return None

    def clean(self):
        now = int(time.time())
        for account in self.accounts.keys():
            for queue in self.accounts[account].keys():
                index = 0
                notify = False
                total = len(self.accounts[account][queue])
                while index < total:
                    message = self.accounts[account][queue][index]
                    if 0 < message['ttl'] <= now:
                        del self.accounts[account][queue][index]
                        total -= 1
                    else:
                        if 0 < message['hide'] <= now:
                            message['hide'] = 0
                            notify = True
                        index += 1
                if notify:
                    self.notify(account, queue)
                if len(self.accounts[account][queue]) == 0:
                    del self.accounts[account][queue]
            if len(self.accounts[account]) == 0:
                del self.accounts[account]

    def _scan_queue(self, account, queue, limit, marker, match_hidden,
                    ttl=None, hide=None, delete=False):
        index = 0
        notify = False
        if marker is not None:
            found = False
            for index in range(0, len(self.accounts[account][queue])):
                message = self.accounts[account][queue][index]
                if message['id'] == marker:
                    index += 1
                    found = True
                    break
            if not found:
                index = 0
        messages = []
        total = len(self.accounts[account][queue])
        while index < total:
            message = self.accounts[account][queue][index]
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
                del self.accounts[account][queue][index]
                total -= 1
            else:
                index += 1
            messages.append(message)
            if limit:
                limit -= 1
                if limit == 0:
                    break
        if notify:
            self.notify(account, queue)
        return messages
