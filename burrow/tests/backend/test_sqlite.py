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

'''Unittests for the sqlite backend.'''

import ConfigParser

import fixtures

import burrow.backend.sqlite
from burrow.tests import backend


class SQLiteBase(backend.Base):
    '''Base test case for sqlite backend.'''

    def setUp(self):
        super(SQLiteBase, self).setUp()
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = burrow.backend.sqlite.Backend(config)
        self.check_empty()


class TestSQLiteAccounts(SQLiteBase, backend.TestAccounts):
    '''Test case for accounts with sqlite backend.'''
    pass


class TestSQLiteQueues(SQLiteBase, backend.TestQueues):
    '''Test case for queues with sqlite backend.'''
    pass


class TestSQLiteMessages(SQLiteBase, backend.TestMessages):
    '''Test case for messages with sqlite backend.'''
    pass


class TestSQLiteMessage(SQLiteBase, backend.TestMessage):
    '''Test case for message with sqlite backend.'''
    pass


class SQLiteFileBase(backend.Base):
    '''Base test case for file-based sqlite backend.'''

    def setUp(self):
        super(SQLiteFileBase, self).setUp()
        tempdir = self.useFixture(fixtures.TempDir()).path
        config = ConfigParser.ConfigParser()
        config.add_section('test')
        config.set('test', 'url', 'sqlite://%s/TestSQLiteFile.db' % tempdir)
        config.set('test', 'synchronous', 'OFF')
        config = (config, 'test')
        self.backend = burrow.backend.sqlite.Backend(config)
        self.check_empty()


class TestSQLiteFileAccounts(SQLiteFileBase, backend.TestAccounts):
    '''Test case for accounts with file-based sqlite backend.'''
    pass


class TestSQLiteFileQueues(SQLiteFileBase, backend.TestQueues):
    '''Test case for queues with file-based sqlite backend.'''
    pass


class TestSQLiteFileMessages(SQLiteFileBase, backend.TestMessages):
    '''Test case for messages with file-based sqlite backend.'''
    pass


class TestSQLiteFileMessage(SQLiteFileBase, backend.TestMessage):
    '''Test case for message with file-based sqlite backend.'''
    pass
