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

# Maximum number of parameters to pass to execute. Testing shows a max of
# 999, so leave a few extra for parameters not added by a list of IDs.
MAXIMUM_PARAMETERS = 990


class Backend(burrow.backend.Backend):

    def __init__(self, config):
        super(Backend, self).__init__(config)
        url = self.config.get('url')
        if url:
            url = urlparse.urlparse(url)
            self.config.set('database', url.netloc)
        database = self.config.get('database', DEFAULT_DATABASE)
        self.db = sqlite3.connect(database)
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

    def delete_accounts(self, filters={}):
        if len(filters) == 0:
            query = 'SELECT rowid FROM accounts LIMIT 1'
            if len(self.db.execute(query).fetchall()) == 0:
                raise burrow.backend.NotFound()
            self.db.execute('DELETE FROM accounts')
            self.db.execute('DELETE FROM queues')
            self.db.execute('DELETE FROM messages')
            return
        count = 0
        detail = self._get_detail(filters)
        ids = []
        query = 'SELECT rowid,account FROM accounts'
        for row in self._get_accounts(query, filters):
            count += 1
            if detail is not None:
                yield self._detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_accounts(ids)
                ids = []
        if count == 0:
            raise burrow.backend.NotFound()
        if len(ids) > 0:
            self._delete_accounts(ids)

    def _delete_accounts(self, ids):
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

    def _get_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.backend.BadDetail(detail)
        return detail

    def _detail(self, row, detail):
        if detail == 'id':
            return row[0]
        return dict(id=row[0])

    def get_accounts(self, filters={}):
        count = 0
        detail = self._get_detail(filters, 'id')
        query = 'SELECT account FROM accounts'
        for row in self._get_accounts(query, filters):
            count += 1
            if detail is not None:
                yield self._detail(row, detail)
        if count == 0:
            raise burrow.backend.NotFound()

    def _get_accounts(self, query, filters):
        values = tuple()
        marker = filters.get('marker', None)
        if marker is not None:
            try:
                marker = self._get_account(marker)
                query += ' WHERE rowid > ?'
                values += (marker,)
            except burrow.backend.NotFound:
                marker = None
        limit = filters.get('limit', None)
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        return self.db.execute(query, values)

    def _get_account(self, account):
        query = 'SELECT rowid FROM accounts WHERE account=?'
        account = self.db.execute(query, (account,)).fetchall()
        if len(account) == 0:
            raise burrow.backend.NotFound()
        return account[0][0]

    def delete_queues(self, account, filters={}):
        account = self._get_account(account)
        count = 0
        detail = self._get_detail(filters)
        ids = []
        query = 'SELECT rowid,queue FROM queues'
        for row in self._get_queues(query, account, filters):
            count += 1
            if detail is not None:
                yield self._detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_queues(ids)
                ids = []
        if count == 0:
            raise burrow.backend.NotFound()
        if len(ids) > 0:
            self._delete_queues(ids)
        query = 'SELECT rowid FROM queues WHERE account=? LIMIT 1'
        if len(self.db.execute(query, (account,)).fetchall()) == 0:
            self.db.execute('DELETE FROM accounts WHERE rowid=?', (account,))

    def _delete_queues(self, ids):
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE queue IN '
        self.db.execute(query + query_values, ids)
        query = 'DELETE FROM queues WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def get_queues(self, account, filters={}):
        account = self._get_account(account)
        count = 0
        detail = self._get_detail(filters, 'id')
        query = 'SELECT queue FROM queues'
        for row in self._get_queues(query, account, filters):
            count += 1
            if detail is not None:
                yield self._detail(row, detail)
        if count == 0:
            raise burrow.backend.NotFound()

    def _get_queues(self, query, account, filters):
        query += ' WHERE account=?'
        values = (account,)
        marker = filters.get('marker', None)
        if marker is not None:
            try:
                marker = self._get_queue(account, marker)
                query += ' AND rowid > ?'
                values += (marker,)
            except burrow.backend.NotFound:
                marker = None
        limit = filters.get('limit', None)
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        return self.db.execute(query, values)

    def _get_queue(self, account, queue):
        query = 'SELECT rowid FROM queues WHERE account=? AND queue=?'
        queue = self.db.execute(query, (account, queue)).fetchall()
        if len(queue) == 0:
            raise burrow.backend.NotFound()
        return queue[0][0]

    def delete_messages(self, account, queue, filters={}):
        account = self._get_account(account)
        queue = self._get_queue(account, queue)
        count = 0
        detail = self._get_message_detail(filters)
        ids = []
        query = 'SELECT rowid,message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue, filters):
            count += 1
            if detail is not None:
                yield self._message_detail(row[1:], detail)
            ids.append(row[0])
            if len(ids) == MAXIMUM_PARAMETERS:
                self._delete_messages(ids)
                ids = []
        if count == 0:
            raise burrow.backend.NotFound()
        if len(ids) > 0:
            self._delete_messages(ids)
        query = 'SELECT rowid FROM messages WHERE queue=? LIMIT 1'
        if len(self.db.execute(query, (queue,)).fetchall()) == 0:
            self.db.execute('DELETE FROM queues WHERE rowid=?', (queue,))
        query = 'SELECT rowid FROM queues WHERE account=? LIMIT 1'
        if len(self.db.execute(query, (account,)).fetchall()) == 0:
            self.db.execute('DELETE FROM accounts WHERE rowid=?', (account,))

    def _delete_messages(self, ids):
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _get_message_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        options = ['id', 'attributes', 'body', 'all']
        if detail == 'none':
            detail = None
        elif detail is not None and detail not in options:
            raise burrow.backend.BadDetail(detail)
        return detail

    def _message_detail(self, row, detail):
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
        return dict(id=row[0], ttl=ttl, hide=hide, body=str(row[3]))

    def get_messages(self, account, queue, filters={}):
        account = self._get_account(account)
        queue = self._get_queue(account, queue)
        count = 0
        detail = self._get_message_detail(filters, 'all')
        query = 'SELECT message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue, filters):
            count += 1
            if detail is not None:
                yield self._message_detail(row, detail)
        if count == 0:
            raise burrow.backend.NotFound()

    def _get_messages(self, query, queue, filters):
        query += ' WHERE queue=?'
        values = (queue,)
        marker = filters.get('marker', None)
        if marker is not None:
            try:
                marker = self._get_message(queue, marker)
                query += ' AND rowid > ?'
                values += (marker,)
            except burrow.backend.NotFound:
                marker = None
        match_hidden = filters.get('match_hidden', False)
        if match_hidden is False:
            query += ' AND hide=0'
        limit = filters.get('limit', None)
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        return self.db.execute(query, values)

    def _get_message(self, queue, message):
        query = 'SELECT rowid FROM messages WHERE queue=? AND message=?'
        queue = self.db.execute(query, (queue, message)).fetchall()
        if len(queue) == 0:
            raise burrow.backend.NotFound()
        return queue[0][0]

    def update_messages(self, account, queue, attributes={}, filters={}):
        account_name, queue_name = account, queue
        account = self._get_account(account)
        queue = self._get_queue(account, queue)
        count = 0
        detail = self._get_message_detail(filters)
        ids = []
        notify = False
        ttl = attributes.get('ttl', None)
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', None)
        if hide is not None and hide > 0:
            hide += int(time.time())
        query = 'SELECT rowid,message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue, filters):
            count += 1
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
        if count == 0:
            raise burrow.backend.NotFound()
        if len(ids) > 0:
            if self._update_messages(ttl, hide, ids):
                notify = True
        if notify:
            self.notify(account_name, queue_name)

    def _update_messages(self, ttl, hide, ids):
        query = 'UPDATE messages SET '
        query_values = ' WHERE rowid IN (?' + (',?' * (len(ids) - 1)) + ')'
        comma = ''
        if ttl is not None:
            query += comma + 'ttl=?'
            ids.insert(0, ttl)
            comma = ','
        if hide is not None:
            query += comma + 'hide=?'
            ids.insert(1, hide)
            comma = ','
        if comma == '':
            return False
        self.db.execute(query + query_values, tuple(ids))
        return True

    def create_message(self, account, queue, message, body, attributes={}):
        try:
            account = self._get_account(account)
        except burrow.backend.NotFound:
            query = 'INSERT INTO accounts VALUES (?)'
            account = self.db.execute(query, (account,)).lastrowid
        query = 'SELECT rowid FROM queues WHERE account=? AND queue=?'
        result = self.db.execute(query, (account, queue)).fetchall()
        if len(result) == 0:
            query = 'INSERT INTO queues VALUES (?, ?)'
            rowid = self.db.execute(query, (account, queue)).lastrowid
        else:
            rowid = result[0][0]
        query = 'SELECT rowid FROM messages WHERE queue=? AND message=?'
        result = self.db.execute(query, (rowid, message)).fetchall()
        ttl = attributes.get('ttl', 0)
        if ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', 0)
        if hide > 0:
            hide += int(time.time())
        if len(result) == 0:
            query = "INSERT INTO messages VALUES (?, ?, ?, ?, ?)"
            self.db.execute(query, (rowid, message, ttl, hide, body))
            self.notify(account, queue)
            return True
        query = "UPDATE messages SET ttl=?, hide=?, body=? WHERE rowid=?"
        self.db.execute(query, (ttl, hide, body, result[0][0]))
        if hide == 0:
            self.notify(account, queue)
        return False

    def delete_message(self, account, queue, message):
        rowid = self._get_queue(self._get_account(account), queue)
        message = self.get_message(account, queue, message)
        if message is None:
            return None
        query = "DELETE FROM messages WHERE queue=%d AND message='%s'" % \
            (rowid, message['id'])
        self.db.execute(query)
        query = "SELECT rowid FROM messages WHERE queue=%d LIMIT 1" % rowid
        if len(self.db.execute(query).fetchall()) == 0:
            query = "DELETE FROM queues WHERE rowid=%d" % rowid
            self.db.execute(query)
        return message

    def get_message(self, account, queue, message):
        rowid = self._get_queue(self._get_account(account), queue)
        query = "SELECT message,ttl,hide,body FROM messages " \
            "WHERE queue=%d AND message='%s'" % (rowid, message)
        result = self.db.execute(query).fetchall()
        if len(result) == 0:
            return None
        row = result[0]
        ttl = row[1]
        if ttl > 0:
            ttl -= int(time.time())
        hide = row[2]
        if hide > 0:
            hide -= int(time.time())
        return dict(id=row[0], ttl=ttl, hide=hide, body=str(row[3]))

    def update_message(self, account, queue, message, attributes):
        rowid = self._get_queue(self._get_account(account), queue)
        message = self.get_message(account, queue, message)
        if message is None:
            return None
        query = "UPDATE messages SET"
        comma = ''
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        if ttl is not None:
            message['ttl'] = ttl
            if ttl > 0:
                ttl += int(time.time())
            query += "%s ttl=%d" % (comma, ttl)
            comma = ','
        if hide is not None:
            message['hide'] = hide
            if hide > 0:
                hide += int(time.time())
            query += "%s hide=%d" % (comma, hide)
            comma = ','
        if comma == '':
            return message
        query += " WHERE queue=%d AND message='%s'" % (rowid, message['id'])
        self.db.execute(query)
        if hide == 0:
            self.notify(account, queue)
        return message

    def clean(self):
        now = int(time.time())
        query = "SELECT rowid,queue FROM messages " \
            "WHERE ttl > 0 AND ttl <= %d" % now
        result = self.db.execute(query).fetchall()
        if len(result) > 0:
            messages = []
            queues = []
            for row in result:
                messages.append(str(row[0]))
                queues.append(row[1])
            query = 'DELETE FROM messages WHERE rowid in (%s)' % \
                ','.join(messages)
            self.db.execute(query)
            for queue in queues:
                query = "SELECT rowid FROM messages WHERE queue=%d LIMIT 1" % \
                    queue
                if len(self.db.execute(query).fetchall()) == 0:
                    query = "DELETE FROM queues WHERE rowid=%d" % queue
                    self.db.execute(query)
        query = "SELECT rowid,queue FROM messages WHERE " \
            "hide > 0 AND hide <= %d" % now
        result = self.db.execute(query).fetchall()
        if len(result) > 0:
            messages = []
            queues = []
            for row in result:
                messages.append(str(row[0]))
                queues.append(row[1])
            query = 'UPDATE messages SET hide=0 WHERE rowid in (%s)' % \
                ','.join(messages)
            self.db.execute(query)
            for queue in queues:
                query = "SELECT account,queue FROM queues WHERE rowid=%d" % \
                    queue
                result = self.db.execute(query).fetchall()[0]
                self.notify(result[0], result[1])
