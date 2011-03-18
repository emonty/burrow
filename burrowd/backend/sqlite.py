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

import sqlite3
import time

import burrowd.backend

# Default configuration values for this module.
DEFAULT_DATABASE = ':memory:'


class Backend(burrowd.backend.Backend):

    def __init__(self, config):
        super(Backend, self).__init__(config)
        database = self.config.get('database', DEFAULT_DATABASE)
        self.db = sqlite3.connect(database)
        queries = [
            'CREATE TABLE queues ('
                'account VARCHAR(255) NOT NULL,'
                'queue VARCHAR(255) NOT NULL,'
                'PRIMARY KEY (account, queue))',
            'CREATE TABLE messages ('
                'queue INT UNSIGNED NOT NULL,'
                'name VARCHAR(255) NOT NULL,'
                'ttl INT UNSIGNED NOT NULL,'
                'hide INT UNSIGNED NOT NULL,'
                'body BLOB NOT NULL,'
                'PRIMARY KEY (queue, name))']
        for query in queries:
            self.db.execute(query)

    def delete_accounts(self, filters={}):
        self.db.execute("DELETE FROM queues")
        self.db.execute("DELETE FROM messages")

    def get_accounts(self, filters={}):
        result = self.db.execute("SELECT account FROM queues").fetchall()
        return [row[0] for row in result]

    def delete_queues(self, account, filters={}):
        query = "SELECT rowid FROM queues WHERE account='%s'" % account
        result = self.db.execute(query).fetchall()
        if len(result) == 0:
            return
        queues = [str(queue[0]) for queue in result]
        query = "DELETE FROM messages WHERE queue IN (%s)" % (','.join(queues))
        self.db.execute(query)
        self.db.execute("DELETE FROM queues WHERE account='%s'" % account)

    def get_queues(self, account, filters={}):
        query = "SELECT queue FROM queues WHERE account='%s'" % account
        result = self.db.execute(query).fetchall()
        return [row[0] for row in result]

    def delete_messages(self, account, queue, filters={}):
        rowid, messages = self._get_messages(account, queue, filters)
        if messages == []:
            return []
        ids = [message['id'] for message in messages]
        query = "DELETE FROM messages WHERE queue=%d AND name IN (%s)" % \
            (rowid, ','.join(ids))
        self.db.execute(query)
        query = "SELECT rowid FROM messages WHERE queue=%d LIMIT 1" % rowid
        if len(self.db.execute(query).fetchall()) == 0:
            query = "DELETE FROM queues WHERE rowid=%d" % rowid
            self.db.execute(query)
        return messages

    def get_messages(self, account, queue, filters={}):
        rowid, messages = self._get_messages(account, queue, filters)
        return messages

    def update_messages(self, account, queue, attributes={}, filters={}):
        rowid, messages = self._get_messages(account, queue, filters)
        if messages == []:
            return []
        query = "UPDATE messages SET"
        comma = ''
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        if ttl is not None:
            query += "%s ttl=%d" % (comma, ttl)
            comma = ','
        if hide is not None:
            query += "%s hide=%d" % (comma, hide)
            comma = ','
        if comma == '':
            return (False, message)
        ids = []
        for message in messages:
            ids.append(message['id'])
            if ttl is not None:
                message['ttl'] = ttl
            if hide is not None:
                message['hide'] = hide
        query += " WHERE queue=%d AND name IN (%s)" % (rowid, ','.join(ids))
        self.db.execute(query)
        self.notify(account, queue)
        return messages

    def create_message(self, account, queue, message, body, attributes):
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
        ttl = attributes.get('ttl', None)
        hide = attributes.get('hide', None)
        if len(result) == 0:
            query = "INSERT INTO messages VALUES (%d, '%s', %d, %d, '%s')" % \
                (rowid, message, ttl, hide, body)
            self.db.execute(query)
            self.notify(account, queue)
            return True
        query = "UPDATE messages SET ttl=%d, hide=%d, body='%s'" \
            "WHERE rowid=%d" % (ttl, hide, body, result[0][0])
        self.db.execute(query)
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
        return dict(id=row[0], ttl=row[1], hide=row[2], body=row[3])

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
            query += "%s ttl=%d" % (comma, ttl)
            comma = ','
        if hide is not None:
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
        query = "SELECT COUNT(*) FROM queues " \
            "WHERE account='%s' AND queue='%s'" % \
            (account, queue)
        result = self.db.execute(query).fetchall()
        if len(result) == 0:
            return None
        return result[0][0]

    def _get_messages(self, account, queue, filters):
        rowid = self._get_queue(account, queue)
        if rowid is None:
            return None, []
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
        messages = []
        for row in result:
            messages.append(dict(id=row[0], ttl=row[1], hide=row[2],
                body=row[3]))
        return rowid, messages
