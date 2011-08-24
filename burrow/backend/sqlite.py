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

'''SQLite backend for burrow.'''

import sqlite3
import time
import urlparse

import burrow.backend

# Default configuration values for this module.
DEFAULT_DATABASE = ':memory:'
DEFAULT_SYNCHRONOUS = 'FULL'

# Maximum number of parameters to pass to execute. Testing shows a max of
# 999, so leave a few extra for parameters not added by a list of IDs.
MAXIMUM_PARAMETERS = 990


class Backend(burrow.backend.Backend):
    '''Backend implemention that uses SQLite to store the account, queue,
    and message data.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        url = self.config.get('url')
        if url:
            url = urlparse.urlparse(url)
            self.config.set('database', url.netloc)
        database = self.config.get('database', DEFAULT_DATABASE)
        self.db = sqlite3.connect(database)
        synchronous = self.config.get('synchronous', DEFAULT_SYNCHRONOUS)
        self.db.execute('PRAGMA synchronous=' + synchronous)
        self.db.isolation_level = None
        queries = [
            'CREATE TABLE IF NOT EXISTS accounts ('
                'account VARCHAR(255) NOT NULL,'
                'PRIMARY KEY (account))',
            'CREATE TABLE IF NOT EXISTS queues ('
                'account INT UNSIGNED NOT NULL,'
                'queue VARCHAR(255) NOT NULL,'
                'PRIMARY KEY (account, queue))',
            'CREATE TABLE IF NOT EXISTS messages ('
                'queue INT UNSIGNED NOT NULL,'
                'message VARCHAR(255) NOT NULL,'
                'ttl INT UNSIGNED NOT NULL,'
                'hide INT UNSIGNED NOT NULL,'
                'body BLOB NOT NULL,'
                'PRIMARY KEY (queue, message))']
        for query in queries:
            self.db.execute(query)

    def delete_accounts(self, filters=None):
        if filters is None or len(filters) == 0:
            query = 'SELECT rowid FROM accounts LIMIT 1'
            if len(self.db.execute(query).fetchall()) == 0:
                raise burrow.NotFound('Account not found')
            self.db.execute('DELETE FROM accounts')
            self.db.execute('DELETE FROM queues')
            self.db.execute('DELETE FROM messages')
            return
        detail = self._get_detail(filters)
        ids = []
        query = 'SELECT rowid,account FROM accounts'
        for row in self._get_accounts(query, filters):
            if detail is not None:
                yield self._detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_accounts(ids)
                ids = []
        if len(ids) > 0:
            self._delete_accounts(ids)

    def _delete_accounts(self, ids):
        '''Delete all accounts with the given row IDs, which includes
        cascading deletes for all queues and messages as well.'''
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        queue_ids = []
        queue_query = 'DELETE FROM messages WHERE queue IN '
        queue_query_values = '(?' + (',?' * (MAXIMUM_PARAMETERS - 1)) + ')'
        query = 'SELECT rowid FROM queues WHERE account IN '
        for row in self.db.execute(query + query_values, ids):
            queue_ids.append(row[0])
            if len(queue_ids) == MAXIMUM_PARAMETERS:
                self.db.execute(queue_query + queue_query_values, queue_ids)
                queue_ids = []
        if len(queue_ids) > 0:
            queue_query_values = '(?' + (',?' * (len(queue_ids) - 1)) + ')'
            self.db.execute(queue_query + queue_query_values, queue_ids)
        query = 'DELETE FROM queues WHERE account IN '
        self.db.execute(query + query_values, ids)
        query = 'DELETE FROM accounts WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _detail(self, row, detail):
        '''Format the account or queue detail from the given row.'''
        if detail == 'id':
            return row[0]
        elif detail == 'all':
            return dict(id=row[0])
        return None

    def get_accounts(self, filters=None):
        detail = self._get_detail(filters, 'id')
        query = 'SELECT account FROM accounts'
        for row in self._get_accounts(query, filters):
            if detail is not None:
                yield self._detail(row, detail)

    def _get_accounts(self, query, filters):
        '''Build the SQL query to get accounts and check for empty
        responses.'''
        values = tuple()
        if filters is None:
            marker = None
            limit = None
        else:
            marker = filters.get('marker', None)
            limit = filters.get('limit', None)
        if marker is not None:
            try:
                marker = self._get_account(marker)
                query += ' WHERE rowid > ?'
                values += (marker,)
            except burrow.NotFound:
                marker = None
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        count = 0
        for row in self.db.execute(query, values):
            count += 1
            yield row
        if count == 0:
            raise burrow.NotFound('Account not found')

    def _get_account(self, account):
        '''Get the rowid for a given account ID.'''
        query = 'SELECT rowid FROM accounts WHERE account=?'
        rows = self.db.execute(query, (account,)).fetchall()
        if len(rows) == 0:
            raise burrow.NotFound('Account not found')
        return rows[0][0]

    def delete_queues(self, account, filters=None):
        account_rowid = self._get_account(account)
        detail = self._get_detail(filters)
        ids = []
        query = 'SELECT rowid,queue FROM queues'
        for row in self._get_queues(query, account_rowid, filters):
            if detail is not None:
                yield self._detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_queues(ids)
                ids = []
        if len(ids) > 0:
            self._delete_queues(ids)
        self._check_empty_account(account_rowid)

    def _delete_queues(self, ids):
        '''Delete all queues with the given row IDs, which includes
        cascading deletes for all messages as well.'''
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE queue IN '
        self.db.execute(query + query_values, ids)
        query = 'DELETE FROM queues WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _check_empty_account(self, account_rowid):
        '''Check to see if an account is empty, and if so, remove it.'''
        query = 'SELECT rowid FROM queues WHERE account=? LIMIT 1'
        if len(self.db.execute(query, (account_rowid,)).fetchall()) == 0:
            query = 'DELETE FROM accounts WHERE rowid=?'
            self.db.execute(query, (account_rowid,))

    def get_queues(self, account, filters=None):
        account_rowid = self._get_account(account)
        detail = self._get_detail(filters, 'id')
        query = 'SELECT queue FROM queues'
        for row in self._get_queues(query, account_rowid, filters):
            if detail is not None:
                yield self._detail(row, detail)

    def _get_queues(self, query, account_rowid, filters):
        '''Build the SQL query to get queues and check for empty
        responses.'''
        query += ' WHERE account=?'
        values = (account_rowid,)
        if filters is None:
            marker = None
            limit = None
        else:
            marker = filters.get('marker', None)
            limit = filters.get('limit', None)
        if marker is not None:
            try:
                marker = self._get_queue(account_rowid, marker)
                query += ' AND rowid > ?'
                values += (marker,)
            except burrow.NotFound:
                marker = None
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        count = 0
        for row in self.db.execute(query, values):
            count += 1
            yield row
        if count == 0:
            raise burrow.NotFound('Queue not found')

    def _get_queue(self, account_rowid, queue):
        '''Get the rowid for a given queue ID.'''
        query = 'SELECT rowid FROM queues WHERE account=? AND queue=?'
        rows = self.db.execute(query, (account_rowid, queue)).fetchall()
        if len(rows) == 0:
            raise burrow.NotFound('Queue not found')
        return rows[0][0]

    @burrow.backend.wait_without_attributes
    def delete_messages(self, account, queue, filters=None):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        detail = self._get_message_detail(filters)
        ids = []
        query = 'SELECT rowid,message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue_rowid, filters):
            if detail is not None:
                yield self._message_detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_messages(ids)
                ids = []
        if len(ids) > 0:
            self._delete_messages(ids)
        self._check_empty_queue(account_rowid, queue_rowid)

    def _delete_messages(self, ids):
        '''Delete all messages with the given row IDs.'''
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _check_empty_queue(self, account_rowid, queue_rowid):
        '''Check to see if a queue is empty, and if so, remove it.'''
        query = 'SELECT rowid FROM messages WHERE queue=? LIMIT 1'
        if len(self.db.execute(query, (queue_rowid,)).fetchall()) == 0:
            self.db.execute('DELETE FROM queues WHERE rowid=?', (queue_rowid,))
            self._check_empty_account(account_rowid)

    def _message_detail(self, row, detail):
        '''Format the message detail from the given row.'''
        if detail == 'id':
            return row[0]
        elif detail == 'body':
            return str(row[3])
        ttl = row[1]
        if ttl > 0:
            ttl -= int(time.time())
        hide = row[2]
        if hide > 0:
            hide -= int(time.time())
        if detail == 'attributes':
            return dict(id=row[0], ttl=ttl, hide=hide)
        elif detail == 'all':
            return dict(id=row[0], ttl=ttl, hide=hide, body=str(row[3]))
        return None

    @burrow.backend.wait_without_attributes
    def get_messages(self, account, queue, filters=None):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        detail = self._get_message_detail(filters, 'all')
        query = 'SELECT message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue_rowid, filters):
            if detail is not None:
                yield self._message_detail(row, detail)

    def _get_messages(self, query, queue_rowid, filters):
        '''Build the SQL query to get messages and check for empty
        responses.'''
        query += ' WHERE queue=?'
        values = (queue_rowid,)
        if filters is None:
            marker = None
            limit = None
            match_hidden = False
        else:
            marker = filters.get('marker', None)
            limit = filters.get('limit', None)
            match_hidden = filters.get('match_hidden', False)
        if marker is not None:
            try:
                marker = self._get_message(queue_rowid, marker)
                query += ' AND rowid > ?'
                values += (marker,)
            except burrow.NotFound:
                marker = None
        if match_hidden is False:
            query += ' AND hide=0'
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        count = 0
        for row in self.db.execute(query, values):
            count += 1
            yield row
        if count == 0:
            raise burrow.NotFound('Message not found')

    def _get_message(self, queue_rowid, message, full=False):
        '''Get the rowid for a given message ID.'''
        if full:
            query = 'SELECT rowid,message,ttl,hide,body'
        else:
            query = 'SELECT rowid'
        query += ' FROM messages WHERE queue=? AND message=?'
        rows = self.db.execute(query, (queue_rowid, message)).fetchall()
        if len(rows) == 0:
            raise burrow.NotFound('Message not found')
        if full:
            return rows[0]
        return rows[0][0]

    @burrow.backend.wait_with_attributes
    def update_messages(self, account, queue, attributes, filters=None):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        detail = self._get_message_detail(filters)
        ids = []
        notify = False
        ttl, hide = self._get_attributes(attributes)
        query = 'SELECT rowid,message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue_rowid, filters):
            if detail is not None:
                row = list(row)
                if ttl is not None:
                    row[2] = ttl
                if hide is not None:
                    row[3] = hide
                yield self._message_detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                if self._update_messages(ttl, hide, ids):
                    notify = True
                ids = []
        if len(ids) > 0:
            if self._update_messages(ttl, hide, ids):
                notify = True
        if notify:
            self.notify(account, queue)

    def _update_messages(self, ttl, hide, ids):
        '''Build the SQL query to update messages.'''
        query = 'UPDATE messages SET '
        query_values = ' WHERE rowid IN (?' + (',?' * (len(ids) - 1)) + ')'
        values = []
        comma = ''
        if ttl is not None:
            query += comma + 'ttl=?'
            values.append(ttl)
            comma = ','
        if hide is not None:
            query += comma + 'hide=?'
            values.append(hide)
            comma = ','
        if comma == '':
            return False
        self.db.execute(query + query_values, tuple(values + ids))
        return True

    def create_message(self, account, queue, message, body, attributes=None):
        ttl, hide = self._get_attributes(attributes, ttl=0, hide=0)
        try:
            account_rowid = self._get_account(account)
        except burrow.NotFound:
            query = 'INSERT INTO accounts VALUES (?)'
            account_rowid = self.db.execute(query, (account,)).lastrowid
        try:
            queue_rowid = self._get_queue(account_rowid, queue)
        except burrow.NotFound:
            query = 'INSERT INTO queues VALUES (?,?)'
            values = (account_rowid, queue)
            queue_rowid = self.db.execute(query, values).lastrowid
        try:
            message_rowid = self._get_message(queue_rowid, message)
            query = 'UPDATE messages SET ttl=?,hide=?,body=? WHERE rowid=?'
            self.db.execute(query, (ttl, hide, body, message_rowid))
            created = False
        except burrow.NotFound:
            query = 'INSERT INTO messages VALUES (?,?,?,?,?)'
            self.db.execute(query, (queue_rowid, message, ttl, hide, body))
            created = True
        if created or hide == 0:
            self.notify(account, queue)
        return created

    def delete_message(self, account, queue, message, filters=None):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters)
        self.db.execute('DELETE FROM messages WHERE rowid=?', (row[0],))
        self._check_empty_queue(account_rowid, queue_rowid)
        return self._message_detail(row[1:], detail)

    def get_message(self, account, queue, message, filters=None):
        queue_rowid = self._get_queue(self._get_account(account), queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters, 'all')
        return self._message_detail(row[1:], detail)

    def update_message(self, account, queue, message, attributes,
        filters=None):
        queue_rowid = self._get_queue(self._get_account(account), queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters)
        ttl, hide = self._get_attributes(attributes)
        if self._update_messages(ttl, hide, [row[0]]):
            self.notify(account, queue)
        row = list(row)
        if ttl is not None:
            row[2] = ttl
        if hide is not None:
            row[3] = hide
        return self._message_detail(row[1:], detail)

    def clean(self):
        now = int(time.time())
        query = 'SELECT rowid,queue FROM messages WHERE ttl > 0 AND ttl <= ?'
        messages = []
        queues = set()
        message_query = 'DELETE FROM messages WHERE rowid IN '
        message_query_values = '(?' + (',?' * (MAXIMUM_PARAMETERS - 1)) + ')'
        for row in self.db.execute(query, (now,)):
            messages.append(row[0])
            if len(messages) == MAXIMUM_PARAMETERS:
                self.db.execute(message_query + message_query_values, messages)
                messages = []
            queues.add(row[1])
        if len(messages) > 0:
            message_query_values = '(?' + (',?' * (len(messages) - 1)) + ')'
            self.db.execute(message_query + message_query_values, messages)
        for queue in queues:
            query = 'SELECT account FROM queues WHERE rowid=?'
            account = self.db.execute(query, (queue,)).fetchall()[0][0]
            self._check_empty_queue(account, queue)
        query = 'SELECT rowid,queue FROM messages WHERE hide > 0 AND hide <= ?'
        messages = []
        queues = set()
        message_query = 'UPDATE messages SET hide=0 WHERE rowid IN '
        message_query_values = '(?' + (',?' * (MAXIMUM_PARAMETERS - 1)) + ')'
        for row in self.db.execute(query, (now,)):
            messages.append(row[0])
            if len(messages) == MAXIMUM_PARAMETERS:
                self.db.execute(message_query + message_query_values, messages)
                messages = []
            queues.add(row[1])
        if len(messages) > 0:
            message_query_values = '(?' + (',?' * (len(messages) - 1)) + ')'
            self.db.execute(message_query + message_query_values, messages)
        for queue in queues:
            query = 'SELECT accounts.account,queues.queue ' \
                'FROM queues JOIN accounts ' \
                'ON queues.account=accounts.rowid ' \
                'WHERE queues.rowid=?'
            result = self.db.execute(query, (queue,)).fetchall()[0]
            self.notify(result[0], result[1])
