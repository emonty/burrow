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

'''Unittests for the WSGI frontend. Most of the WSGI server is tested by
the HTTP backend, so this covers things that don't translate directly to
the Python API.'''

import httplib
import json
import unittest


class TestWSGI(unittest.TestCase):
    '''Test case for WSGI frontend.'''

    def test_versions(self):
        connection = httplib.HTTPConnection('localhost', 8080)
        connection.request('GET', '/')
        response = connection.getresponse()
        self.assertEquals(response.status, 200)
        body = json.loads(response.read())
        self.assertEquals(body, ['v1.0'])

    def test_unknown_method(self):
        connection = httplib.HTTPConnection('localhost', 8080)
        connection.request('OPTIONS', '/v1.0')
        response = connection.getresponse()
        self.assertEquals(response.status, 405)

    def test_unknown_url(self):
        connection = httplib.HTTPConnection('localhost', 8080)
        connection.request('GET', '/unknown')
        response = connection.getresponse()
        self.assertEquals(response.status, 404)
