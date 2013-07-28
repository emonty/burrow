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

'''Backends for burrow.'''

import time

import eventlet

import burrow.common

# Since this is an interface, arguments are unused. Ignore warnings in pylint.
# pylint: disable=W0613


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
        '''Delete accounts, which includes all queues and messages within
        the accounts. With no filters, this will delete all data for the
        entire server, so it should be used with caution.

        :param filters: Optional dict of filters for the request. Valid
            filters are 'marker', 'limit', and 'detail'. The 'marker'
            value is the last seen account ID for use in pagination,
            and only accounts after this ID will be affected. If the
            'marker' value is not given or not found, it will start from
            the beginning. The 'limit' value is the number of accounts
            to delete for the request. If 'limit' is not given, it will
            delete them all. Valid values for 'detail' are 'none', 'id',
            and 'all'. Default value for 'detail' is 'none'.

        :returns: Generator which will loop through all accounts if
            'detail' is not 'none'. If 'detail' is 'none', the generator
            will stop on the first iteration, but it still must be called
            to finish the request.
        '''
        return []

    def get_accounts(self, filters=None):
        '''Get a list of accounts, possibly filtered. This uses the same
        parameters and return type as :func:`delete_accounts()`, except
        the default value for 'detail' in 'filters' is 'id'.'''
        return []

    def delete_queues(self, account, filters=None):
        '''Delete queues within an account, which includes all messages
        within the queues. With no filters, this will delete all data
        for the entire account, so it should be used with caution.

        :param account: Account to delete the queues from.

        :param filters: Optional dict of filters for the request. Valid
            filters are 'marker', 'limit', and 'detail'. The 'marker'
            value is the last seen queue ID for use in pagination, and
            only queues after this ID will be affected. If the 'marker'
            value is not given or not found, it will start from the
            beginning. The 'limit' value is the number of queues to
            delete for the request. If 'limit' is not given, it will
            delete them all. Valid values for 'detail' are 'none', 'id',
            and 'all'. Default value for 'detail' is 'none'.

        :returns: Generator which will loop through all queues if 'detail'
            is not 'none'. If 'detail' is 'none', the generator will stop
            on the first iteration, but it still must be called to finish
            the request.
        '''
        return []

    def get_queues(self, account, filters=None):
        '''Get a list of queues, possibly filtered. This uses the same
        parameters and return type as :func:`delete_queues()`, except
        the default value for 'detail' in 'filters' is 'id'.'''
        return []

    def delete_messages(self, account, queue, filters=None):
        '''Delete messages within a queue. With no filters, this will
        delete all messages in the queue, so it should be used with
        caution.

        :param account: Account to delete the messages from.

        :param queue: Queue within the given account to delete the
            messages from.

        :param filters: Optional dict of filters for the request. Valid
            filters are 'marker', 'limit', 'match_hidden', 'wait', and
            'detail'. The 'marker' value is the last seen message ID for
            use in pagination, and only messages after this ID will be
            affected. If the 'marker' value is not given or not found,
            it will start from the beginning. The 'limit' value is the
            number of messages to delete for the request. If 'limit'
            is not given, it will delete them all. If 'match_hidden'
            is True, the request will match all messages, even if their
            'hide' value is non-zero, otherwise messages with a 'hide'
            value of non-zero are skipped. If 'wait' is given, this is the
            number of seconds for the request to wait for a message if no
            messages can be found. Valid values for 'detail' are 'none',
            'id', 'attributes', 'body', and 'all'. Default value for
            'detail' is 'none'.

        :returns: Generator which will loop through all messages if 'detail'
            is not 'none'. If 'detail' is 'none', the generator will stop
            on the first iteration, but it still must be called to finish
            the request.
        '''
        return []

    def get_messages(self, account, queue, filters=None):
        '''Get a list of messages, possibly filtered. This uses the same
        parameters and return type as :func:`delete_messages()`, except
        the default value for 'detail' in 'filters' is 'all'.'''
        return []

    def update_messages(self, account, queue, attributes, filters=None):
        '''Update a list of messages, possibly filtered. In addition to
        the parameters and return type used in :func:`delete_messages()`,
        this also requires:

        :param attributes: Attributes to set as described in
            :func:`create_message()`
        '''
        return []

    def create_message(self, account, queue, message, body, attributes=None):
        '''Create a new message in the given account and queue.

        :param account: Account to create the messages in.

        :param queue: Queue within the given account to create the
            messages in.

        :param message: Message ID to use within the queue, which is
            always unique. When a message of the same ID already exists,
            the old message is overwritten with this new one.

        :param body: Body of the message.

        :param attributes: A dict of initial attributes to set. Valid
            attributes are 'ttl' and 'hide'. The value of 'ttl' is the
            number of seconds from when the request is made for the message
            to be removed automatically by the server. The value of 'hide'
            is the number of seconds from when the request is made for
            the message to stay hidden from requests that do not have the
            'match_hidden' filter given as True.

        :returns: True if the message was created, False if a message
            with the same ID existed and was replaced.
        '''
        return True

    def delete_message(self, account, queue, message, filters=None):
        '''Same as :func:`delete_messages()`, except only delete the
        given message ID.

        :returns: The message detail according to 'detail' in 'filters,
            or None if 'detail' is 'none'.
        '''
        return None

    def get_message(self, account, queue, message, filters=None):
        '''Same as :func:`get_messages()`, except only get the given
        message ID.

        :returns: The message detail according to 'detail' in 'filters,
            or None if 'detail' is 'none'.
        '''
        return None

    def update_message(self, account, queue, message, attributes,
        filters=None):
        '''Same as :func:`update_messages()`, except only update the
        given message ID.

        :returns: The message detail according to 'detail' in 'filters,
            or None if 'detail' is 'none'.
        '''
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
        '''Helper method to parse attributes for implementations to use.'''
        if attributes is not None:
            ttl = attributes.get('ttl', ttl)
            hide = attributes.get('hide', hide)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        if hide is not None and hide > 0:
            hide += int(time.time())
        return ttl, hide

    def _get_detail(self, filters, default=None):
        '''Helper method to parse account and queue detail for
        implementations to use.'''
        detail = default if filters is None else filters.get('detail', default)
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.InvalidArguments(detail)
        return detail

    def _get_message_detail(self, filters, default=None):
        '''Helper method to parse message detail for implementations
        to use.'''
        detail = default if filters is None else filters.get('detail', default)
        options = ['id', 'attributes', 'body', 'all']
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in options:
            raise burrow.InvalidArguments(detail)
        return detail

    def notify(self, account, queue):
        '''Notify any waiting callers that the account/queue has
        a visible message.'''
        queue = '%s/%s' % (account, queue)
        if queue in self.queues:
            for _count in xrange(0, self.queues[queue].getting()):
                self.queues[queue].put(0)

    def wait(self, account, queue, seconds):
        '''Wait for a message to appear in the account/queue.'''
        queue = '%s/%s' % (account, queue)
        if queue not in self.queues:
            self.queues[queue] = eventlet.Queue()
        try:
            self.queues[queue].get(timeout=seconds)
        except eventlet.queue.Empty:
            pass
        if self.queues[queue].getting() == 0:
            del self.queues[queue]


def wait_without_attributes(method):
    '''Decorator that will wait for messages with the method does not
    take attributes.'''
    def __wrapper__(self, account, queue, filters=None):
        original = lambda: method(self, account, queue, filters)
        return wait(self, account, queue, filters, original)
    return __wrapper__


def wait_with_attributes(method):
    '''Decorator that will wait for messages with the method takes
    attributes.'''
    def __wrapper__(self, account, queue, attributes, filters=None):
        original = lambda: method(self, account, queue, attributes, filters)
        return wait(self, account, queue, filters, original)
    return __wrapper__


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
        except burrow.NotFound as exception:
            now = time.time()
            if seconds - now > 0:
                self.wait(account, queue, seconds - now)
            if seconds < time.time():
                raise exception
