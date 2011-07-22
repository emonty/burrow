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
            'CREATE TABLE IF NOT EXISTS queues ('
                'account VARCHAR(255) NOT NULL,'
                'queue VARCHAR(255) NOT NULL,'
                'PRIMARY KEY (account, queue))',
            'CREATE TABLE IF NOT EXISTS messages ('
                'queue INT UNSIGNED NOT NULL,'
                'name VARCHAR(255) NOT NULL,'
                'ttl INT UNSIGNED NOT NULL,'
                'hide INT UNSIGNED NOT NULL,'
                'body BLOB NOT NULL,'
                'PRIMARY KEY (queue, name))']
        for query in queries:
            self.db.execute(query)

    def delete_accounts(self, filters={}):
        if len(filters) == 0:
            self.db.execute('DELETE FROM queues')
            self.db.execute('DELETE FROM messages')
            return
        query = 'SELECT rowid,account FROM queues'
        values = tuple()
        marker = filters.get('marker', None)
        if marker is not None:
            query += ' WHERE account >= ?'
            values += (marker,)
        limit = filters.get('limit', None)
        detail = self._get_detail(filters, 'id')
        current_account = None
        ids = []
        marker_found = False
        for row in self.db.execute(query, values):
            if marker == row[1]:
                marker_found = True
                continue
            elif marker is not None and not marker_found:
                break
            if current_account != row[1]:
                if limit is not None:
                    if limit == 0:
                        break
                    limit -= 1
                current_account = row[1]
                if detail is 'id':
                    yield row[1]
                elif detail is 'all':
                    yield dict(id=row[1])
            ids.append(row[0])
            if len(ids) == 999:
                self._delete_queues(ids)
                ids = []
        if marker is not None and not marker_found:
            filters = dict(filters)
            filters.pop('marker')
            for account in self.delete_accounts(filters):
                yield account
        if len(ids) > 0:
            self._delete_queues(ids)

    def get_accounts(self, filters={}):
        query = 'SELECT DISTINCT account FROM queues'
        values = tuple()
        limit = filters.get('limit', None)
        marker = filters.get('marker', None)
        if marker is not None:
            query += ' WHERE account >= ?'
            values += (marker,)
            if limit is not None:
                limit += 1
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        detail = self._get_detail(filters, 'id')
        marker_found = False
        for row in self.db.execute(query, values):
            if marker == row[0]:
                marker_found = True
                continue
            elif marker is not None and not marker_found:
                break
            if detail is 'id':
                yield row[0]
            elif detail is 'all':
                yield dict(id=row[0])
        if marker is not None and not marker_found:
            filters = dict(filters)
            filters.pop('marker')
            for account in self.get_accounts(filters):
                yield account

    def delete_queues(self, account, filters={}):
        query = 'SELECT rowid,queue FROM queues WHERE account=?'
        values = (account,)
        query, values, marker = self._add_queue_filters(query, values, filters)
        detail = self._get_detail(filters, None)
        ids = []
        marker_found = False
        for row in self.db.execute(query, values):
            if marker == row[1]:
                marker_found = True
                continue
            elif marker is not None and not marker_found:
                break
            if detail is 'id':
                yield row[1]
            elif detail is 'all':
                yield dict(id=row[1])
            ids.append(row[0])
            if len(ids) == 999:
                self._delete_queues(ids)
                ids = []
        if marker is not None and not marker_found:
            filters = dict(filters)
            filters.pop('marker')
            for queue in self.delete_queues(account, filters):
                yield queue
        if len(ids) > 0:
            self._delete_queues(ids)

    def get_queues(self, account, filters={}):
        query = 'SELECT queue FROM queues WHERE account=?'
        values = (account,)
        query, values, marker = self._add_queue_filters(query, values, filters)
        detail = self._get_detail(filters, 'id')
        marker_found = False
        for row in self.db.execute(query, values):
            if marker == row[0]:
                marker_found = True
                continue
            elif marker is not None and not marker_found:
                break
            if detail is 'id':
                yield row[0]
            elif detail is 'all':
                yield dict(id=row[0])
        if marker is not None and not marker_found:
            filters = dict(filters)
            filters.pop('marker')
            for queue in self.get_queues(account, filters):
                yield queue

    def _add_queue_filters(self, query, values, filters):
        limit = filters.get('limit', None)
        marker = filters.get('marker', None)
        if marker is not None:
            query += ' AND queue >= ?'
            values += (marker,)
            if limit is not None:
                limit += 1
        if limit is not None:
            query += ' LIMIT ?'
            values += (limit,)
        return query, values, marker

    def _delete_queues(self, ids):
        query = 'DELETE FROM messages WHERE queue IN (?' + \
            (',?' * (len(ids) - 1)) + ')'
        self.db.execute(query, tuple(ids))
        query = 'DELETE FROM queues WHERE rowid IN (?' + \
            (',?' * (len(ids) - 1)) + ')'
        self.db.execute(query, tuple(ids))

    def _get_detail(self, filters, default=None):
        detail = filters.get('detail', default)
        if detail is 'none':
            detail = None
        elif detail is not None and detail not in ['id', 'all']:
            raise burrow.backend.BadDetail(detail)
        return detail

    def delete_messages(self, account, queue, filters={}):
        result = self._get_messages(account, queue, filters)
        rowid = result.next()
        ids = []
        for message in result:
            ids.append(message['id'])
            yield message
        if len(ids) == 0:
            return
        values = (rowid,) + tuple(ids)
        query = 'DELETE FROM messages WHERE queue=? AND name IN (%s)'
        self.db.execute(query % ','.join('?' * len(ids)), values)
        query = 'SELECT rowid FROM messages WHERE queue=? LIMIT 1'
        if len(self.db.execute(query, (rowid,)).fetchall()) == 0:
            query = 'DELETE FROM queues WHERE rowid=?'
            self.db.execute(query, (rowid,))

    def get_messages(self, account, queue, filters={}):
        result = self._get_messages(account, queue, filters)
        rowid = result.next()
        return result

    def update_messages(self, account, queue, attributes={}, filters={}):
        result = self._get_messages(account, queue, filters)
        rowid = result.next()
        ids = []
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        for message in result:
            ids.append(message['id'])
            if ttl is not None:
                message['ttl'] = ttl
            if hide is not None:
                message['hide'] = hide
            yield message
        if len(ids) == 0:
            return
        query = 'UPDATE messages SET'
        comma = ''
        values = tuple()
        if ttl is not None:
            if ttl > 0:
                ttl += int(time.time())
            query += '%s ttl=?' % comma
            values += (ttl,)
            comma = ','
        if hide is not None:
            if hide > 0:
                hide += int(time.time())
            query += '%s hide=?' % comma
            values += (hide,)
            comma = ','
        if comma == '':
            return
        values += (rowid,)
        values += tuple(ids)
        query += ' WHERE queue=? AND name IN (%s)'
        self.db.execute(query % ','.join('?' * len(ids)), values)
        self.notify(account, queue)

    def create_message(self, account, queue, message, body, attributes={}):
        query = "SELECT rowid FROM queues " \
            "WHERE account='%s' AND queue='%s'" % (account, queue)
        result = self.db.execute(query).fetchall()
        if len(result) == 0:
            query = "INSERT INTO queues VALUES ('%s', '%s')" % (account, queue)
            rowid = self.db.execute(query).lastrowid
        else:
            rowid = result[0][0]
        query = "SELECT rowid FROM messages WHERE queue=%d AND name='%s'" % \
            (rowid, message)
        result = self.db.execute(query).fetchall()
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
        rowid = self._get_queue(account, queue)
        if rowid is None:
            return None
        message = self.get_message(account, queue, message)
        if message is None:
            return None
        query = "DELETE FROM messages WHERE queue=%d AND name='%s'" % \
            (rowid, message['id'])
        self.db.execute(query)
        query = "SELECT rowid FROM messages WHERE queue=%d LIMIT 1" % rowid
        if len(self.db.execute(query).fetchall()) == 0:
            query = "DELETE FROM queues WHERE rowid=%d" % rowid
            self.db.execute(query)
        return message

    def get_message(self, account, queue, message):
        rowid = self._get_queue(account, queue)
        if rowid is None:
            return None
        query = "SELECT name,ttl,hide,body FROM messages " \
            "WHERE queue=%d AND name='%s'" % (rowid, message)
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
        rowid = self._get_queue(account, queue)
        if rowid is None:
            return None
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
        query += " WHERE queue=%d AND name='%s'" % (rowid, message['id'])
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

    def _get_queue(self, account, queue):
        query = "SELECT rowid FROM queues " \
            "WHERE account='%s' AND queue='%s'" % \
            (account, queue)
        result = self.db.execute(query).fetchall()
        if len(result) == 0:
            return None
        return result[0][0]

    def _get_messages(self, account, queue, filters):
        rowid = self._get_queue(account, queue)
        yield rowid
        if rowid is None:
            return
        marker = None
        if 'marker' in filters and filters['marker'] is not None:
            query = "SELECT rowid FROM messages " \
                "WHERE queue=%d AND name='%s'" % (rowid, filters['marker'])
            result = self.db.execute(query).fetchall()
            if len(result) > 0:
                marker = result[0][0]
        query = "SELECT name,ttl,hide,body FROM messages WHERE queue=%d" % \
            rowid
        if marker is not None:
            query += " AND rowid > %d" % marker
        if 'match_hidden' not in filters or filters['match_hidden'] is False:
            query += " AND hide == 0"
        if 'limit' in filters and filters['limit'] is not None:
            query += " LIMIT %d" % filters['limit']
        result = self.db.execute(query).fetchall()
        for row in result:
            ttl = row[1]
            if ttl > 0:
                ttl -= int(time.time())
            hide = row[2]
            if hide > 0:
                hide -= int(time.time())
            yield dict(id=row[0], ttl=ttl, hide=hide, body=str(row[3]))
