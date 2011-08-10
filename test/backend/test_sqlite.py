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

import ConfigParser
import os

import burrow.backend.sqlite
import test.backend.test_memory


class TestSQLite(test.backend.test_memory.TestMemory):
    '''Unittests for the memory-based SQLite backend.'''

    def setUp(self):
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = burrow.backend.sqlite.Backend(config)
        self.check_empty()


class TestSQLiteFile(test.backend.test_memory.TestMemory):
    '''Unittests for the file-based SQLite backend.'''

    def setUp(self):
        try:
            os.unlink('TestSQLiteFile.db')
        except OSError:
            pass
        config = ConfigParser.ConfigParser()
        config.add_section('test')
        config.set('test', 'url', 'sqlite://TestSQLiteFile.db')
        config.set('test', 'synchronous', 'OFF')
        config = (config, 'test')
        self.backend = burrow.backend.sqlite.Backend(config)
        self.check_empty()

    def tearDown(self):
        self.check_empty()
        os.unlink('TestSQLiteFile.db')
