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
        rows = self.db.execute(query, (account,)).fetchall()
        if len(rows) == 0:
            raise burrow.backend.NotFound()
        return rows[0][0]

    def delete_queues(self, account, filters={}):
        account_rowid = self._get_account(account)
        count = 0
        detail = self._get_detail(filters)
        ids = []
        query = 'SELECT rowid,queue FROM queues'
        for row in self._get_queues(query, account_rowid, filters):
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
        self._check_empty_account(account_rowid)

    def _delete_queues(self, ids):
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE queue IN '
        self.db.execute(query + query_values, ids)
        query = 'DELETE FROM queues WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _check_empty_account(self, account_rowid):
        query = 'SELECT rowid FROM queues WHERE account=? LIMIT 1'
        if len(self.db.execute(query, (account_rowid,)).fetchall()) == 0:
            query = 'DELETE FROM accounts WHERE rowid=?'
            self.db.execute(query, (account_rowid,))

    def get_queues(self, account, filters={}):
        account_rowid = self._get_account(account)
        count = 0
        detail = self._get_detail(filters, 'id')
        query = 'SELECT queue FROM queues'
        for row in self._get_queues(query, account_rowid, filters):
            count += 1
            if detail is not None:
                yield self._detail(row, detail)
        if count == 0:
            raise burrow.backend.NotFound()

    def _get_queues(self, query, account_rowid, filters):
        query += ' WHERE account=?'
        values = (account_rowid,)
        marker = filters.get('marker', None)
        if marker is not None:
            try:
                marker = self._get_queue(account_rowid, marker)
                query += ' AND rowid > ?'
                values += (marker,)
            except burrow.backend.NotFound:
                marker = None
        limit = filters.get('limit', None)
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        return self.db.execute(query, values)

    def _get_queue(self, account_rowid, queue):
        query = 'SELECT rowid FROM queues WHERE account=? AND queue=?'
        rows = self.db.execute(query, (account_rowid, queue)).fetchall()
        if len(rows) == 0:
            raise burrow.backend.NotFound()
        return rows[0][0]

    def delete_messages(self, account, queue, filters={}):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        count = 0
        detail = self._get_message_detail(filters)
        ids = []
        query = 'SELECT rowid,message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue_rowid, filters):
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
        self._check_empty_queue(account_rowid, queue_rowid)

    def _delete_messages(self, ids):
        ids = tuple(ids)
        query_values = '(?' + (',?' * (len(ids) - 1)) + ')'
        query = 'DELETE FROM messages WHERE rowid IN '
        self.db.execute(query + query_values, ids)

    def _check_empty_queue(self, account_rowid, queue_rowid):
        query = 'SELECT rowid FROM messages WHERE queue=? LIMIT 1'
        if len(self.db.execute(query, (queue_rowid,)).fetchall()) == 0:
            self.db.execute('DELETE FROM queues WHERE rowid=?', (queue_rowid,))
            self._check_empty_account(account_rowid)

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
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        count = 0
        detail = self._get_message_detail(filters, 'all')
        query = 'SELECT message,ttl,hide,body FROM messages'
        for row in self._get_messages(query, queue_rowid, filters):
            count += 1
            if detail is not None:
                yield self._message_detail(row, detail)
        if count == 0:
            raise burrow.backend.NotFound()

    def _get_messages(self, query, queue_rowid, filters):
        query += ' WHERE queue=?'
        values = (queue_rowid,)
        marker = filters.get('marker', None)
        if marker is not None:
            try:
                marker = self._get_message(queue_rowid, marker)
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

    def _get_message(self, queue_rowid, message, full=False):
        if full:
            query = 'SELECT rowid,message,ttl,hide,body'
        else:
            query = 'SELECT rowid'
        query += ' FROM messages WHERE queue=? AND message=?'
        values = (queue_rowid, message)
        rows = self.db.execute(query, values).fetchall()
        if len(rows) == 0:
            raise burrow.backend.NotFound()
        if full:
            return rows[0]
        return rows[0][0]

    def update_messages(self, account, queue, attributes, filters={}):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
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
        for row in self._get_messages(query, queue_rowid, filters):
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
            self.notify(account, queue)

    def _update_messages(self, ttl, hide, ids):
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

    def create_message(self, account, queue, message, body, attributes={}):
        try:
            account_rowid = self._get_account(account)
        except burrow.backend.NotFound:
            query = 'INSERT INTO accounts VALUES (?)'
            account_rowid = self.db.execute(query, (account,)).lastrowid
        try:
            queue_rowid = self._get_queue(account_rowid, queue)
        except burrow.backend.NotFound:
            query = 'INSERT INTO queues VALUES (?, ?)'
            values = (account_rowid, queue)
            queue_rowid = self.db.execute(query, values).lastrowid
        ttl = attributes.get('ttl', 0)
        if ttl > 0:
            ttl += int(time.time())
        hide = attributes.get('hide', 0)
        if hide > 0:
            hide += int(time.time())
        query = 'SELECT rowid FROM messages WHERE queue=? AND message=?'
        values = (queue_rowid, message)
        message_rowid = self.db.execute(query, values).fetchall()
        if len(message_rowid) == 0:
            query = 'INSERT INTO messages VALUES (?,?,?,?,?)'
            self.db.execute(query, (queue_rowid, message, ttl, hide, body))
            self.notify(account, queue)
            return True
        query = 'UPDATE messages SET ttl=?,hide=?,body=? WHERE rowid=?'
        self.db.execute(query, (ttl, hide, body, message_rowid[0][0]))
        if hide == 0:
            self.notify(account, queue)
        return False

    def delete_message(self, account, queue, message, filters={}):
        account_rowid = self._get_account(account)
        queue_rowid = self._get_queue(account_rowid, queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters)
        self.db.execute('DELETE FROM messages WHERE rowid=?', (row[0],))
        self._check_empty_queue(account_rowid, queue_rowid)
        if detail is None:
            return None
        return self._message_detail(row[1:], detail)

    def get_message(self, account, queue, message, filters={}):
        queue_rowid = self._get_queue(self._get_account(account), queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters, 'all')
        if detail is None:
            return None
        return self._message_detail(row[1:], detail)

    def update_message(self, account, queue, message, attributes, filters={}):
        queue_rowid = self._get_queue(self._get_account(account), queue)
        row = self._get_message(queue_rowid, message, True)
        detail = self._get_message_detail(filters)
        query = 'UPDATE messages SET'
        values = tuple()
        comma = ''
        ttl = attributes.get('ttl', None)
        row = list(row)
        if ttl is not None:
            row[2] = ttl
            if ttl > 0:
                ttl += int(time.time())
            query += comma + ' ttl=?'
            values += (ttl,)
            comma = ','
        hide = attributes.get('hide', None)
        if hide is not None:
            row[3] = hide
            if hide > 0:
                hide += int(time.time())
            query += comma + ' hide=?'
            values += (hide,)
            comma = ','
        if comma != '':
            query += ' WHERE rowid=?'
            values += (row[0],)
            self.db.execute(query, values)
            if hide == 0:
                self.notify(account, queue)
        if detail is None:
            return None
        return self._message_detail(row[1:], detail)

    def clean(self):
        now = int(time.time())
        query = 'SELECT rowid,queue FROM messages ' \
            'WHERE ttl > 0 AND ttl <= ?'
        result = self.db.execute(query, (now,)).fetchall()
        if len(result) > 0:
            messages = []
            queues = []
            for row in result:
                messages.append(str(row[0]))
                queues.append(row[1])
            query = 'DELETE FROM messages WHERE rowid in (%s)' % \
                ','.join(messages)
            self.db.execute(query)
            for queue_rowid in queues:
                query = 'SELECT account FROM queues WHERE rowid=?'
                account_rowid = self.db.execute(query, (queue_rowid,))
                account_rowid = account_rowid.fetchall()[0][0]
                self._check_empty_queue(account_rowid, queue_rowid)
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
                query = 'SELECT accounts.account,queues.queue ' \
                    'FROM queues JOIN accounts ' \
                    'ON queues.account=accounts.rowid ' \
                    'WHERE queues.rowid=?'
                result = self.db.execute(query, (queue,)).fetchall()[0]
                self.notify(result[0], result[1])
