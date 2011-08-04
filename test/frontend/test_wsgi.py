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
import json
import time
import unittest

import eventlet
import webob

import burrow.backend.memory
import burrow.backend.sqlite
import burrow.frontend.wsgi


class TestWSGIMemory(unittest.TestCase):
    '''Unittests for the WSGI frontend to SQLite backend.'''
    backend_class = burrow.backend.memory.Backend

    def setUp(self):
        config = (ConfigParser.ConfigParser(), 'test')
        self.backend = self.backend_class(config)
        self.frontend = burrow.frontend.wsgi.Frontend(config, self.backend)
        self.frontend.default_ttl = 0
        self._get_url('', status=404)
        self._get_url('/a', status=404)
        self._get_url('/a/q', status=404)

    def tearDown(self):
        self._get_url('/a/q', status=404)
        self._get_url('/a', status=404)
        self._get_url('', status=404)

    def test_account(self):
        self._put_url('/a/q/1')
        result = self._get_url('')
        self.assertEquals(result, ['a'])
        self._delete_url('/a')

    def test_queue(self):
        self._put_url('/a/q/1')
        result = self._get_url('/a')
        self.assertEquals(result, ['q'])
        self._delete_url('/a/q')

    def test_message(self):
        self._put_url('/a/q/1', body='b')
        result = self._get_url('/a/q')
        self.assertMessages(result, [self.message('1', body='b')])
        self._delete_url('/a/q/1')

    def test_message_post(self):
        self._put_url('/a/q/1', body='b')
        for x in range(0, 3):
            result = self._post_url('/a/q/1?ttl=%d&hide=%d' % (x, x))
            self.assertEquals(result, {'id': '1'})
            result = self._get_url('/a/q?match_hidden=true')
            message = self.message('1', x, x, body='b')
            self.assertMessages(result, [message])
        self._delete_url('/a/q/1')

    def test_message_put(self):
        for x in range(0, 3):
            url = '/a/q/1?ttl=%d&hide=%d' % (x, x)
            status = 201 if x == 0 else 204
            self._put_url(url, body=str(x), status=status)
            result = self._get_url('/a/q?match_hidden=true')
            message = self.message('1', x, x, body=str(x))
            self.assertMessages(result, [message])
        self._delete_url('/a/q/1')

    def test_message_delete_limit(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        result = self._delete_url('/a/q?limit=3&detail=all', status=200)
        messages = []
        messages.append(self.message('1'))
        messages.append(self.message('2'))
        messages.append(self.message('3'))
        self.assertMessages(result, messages)
        result = self._delete_url('/a/q?limit=3&detail=all', status=200)
        message = self.message('4')
        self.assertMessages(result, [message])

    def test_message_get_limit(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            result = self._get_url('/a/q?limit=3')
            messages = []
            for y in range(x, 4)[:3]:
                messages.append(self.message(str(y + 1)))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_post_limit(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            result = self._post_url('/a/q?limit=3&ttl=%d&detail=all' % x)
            messages = []
            for y in range(x, 4)[:3]:
                messages.append(self.message(str(y + 1), x))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_delete_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        result = self._delete_url('/a/q?marker=2&detail=all', status=200)
        messages = []
        messages.append(self.message('3'))
        messages.append(self.message('4'))
        self.assertMessages(result, messages)
        result = self._delete_url('/a/q?marker=5&detail=all', status=200)
        messages = []
        messages.append(self.message('1'))
        messages.append(self.message('2'))
        self.assertMessages(result, messages)

    def test_message_get_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            result = self._get_url('/a/q?marker=%d' % x)
            messages = []
            for y in range(x, 4):
                messages.append(self.message(str(y + 1)))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_post_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            url = '/a/q?marker=%d&ttl=%d&detail=all' % (x, x)
            result = self._post_url(url)
            messages = []
            for y in range(x, 4):
                messages.append(self.message(str(y + 1), x))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_delete_limit_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        url = '/a/q?limit=2&marker=1&detail=all'
        result = self._delete_url(url, status=200)
        messages = []
        messages.append(self.message('2'))
        messages.append(self.message('3'))
        self.assertMessages(result, messages)
        url = '/a/q?limit=2&marker=5&detail=all'
        result = self._delete_url(url, status=200)
        messages = []
        messages.append(self.message('1'))
        messages.append(self.message('4'))
        self.assertMessages(result, messages)

    def test_message_get_limit_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            result = self._get_url('/a/q?limit=2&marker=%d' % x)
            messages = []
            for y in range(x, 4)[:2]:
                messages.append(self.message(str(y + 1)))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_post_limit_marker(self):
        [self._put_url('/a/q/%d' % x) for x in range(1, 5)]
        for x in range(0, 4):
            url = '/a/q?limit=2&marker=%d&ttl=%d&detail=all' % (x, x)
            result = self._post_url(url)
            messages = []
            for y in range(x, 4)[:2]:
                messages.append(self.message(str(y + 1), x))
            self.assertMessages(result, messages)
            self._delete_url('/a/q/%d' % (x + 1))

    def test_message_ttl(self):
        self._put_url('/a/q/1?ttl=1')
        result = self._get_url('/a/q/1')
        message = self.message('1', 1)
        self.assertMessages([result], [self.message('1', 1)])
        time.sleep(1)
        self.backend.clean()
        self._get_url('/a/q/1', status=404)
        self._put_url('/a/q/1')
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1')])
        self._post_url('/a/q/1?ttl=1')
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1', 1)])
        time.sleep(1)
        self.backend.clean()
        self._get_url('/a/q/1', status=404)

    def test_message_hide(self):
        self._put_url('/a/q/1?hide=1')
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1', hide=1)])
        time.sleep(1)
        self.backend.clean()
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1')])
        self._post_url('/a/q/1?hide=1')
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1', hide=1)])
        time.sleep(1)
        self.backend.clean()
        result = self._get_url('/a/q/1')
        self.assertMessages([result], [self.message('1')])
        self._delete_url('/a/q/1')

    def _message_wait(self):
        result = self._get_url('/a/q?wait=2')
        self.assertMessages(result, [self.message('1')])
        self.success = True

    def test_message_put_wait(self):
        self.success = False
        thread = eventlet.spawn(self._message_wait)
        eventlet.spawn_after(0.2, self._put_url, '/a/q/1')
        thread.wait()
        self.assertTrue(self.success)
        self._delete_url('/a/q/1')

    def test_message_put_wait_overwrite(self):
        self.success = False
        self._put_url('/a/q/1?hide=10')
        thread = eventlet.spawn(self._message_wait)
        eventlet.spawn_after(0.2, self._put_url, '/a/q/1?hide=0', status=204)
        thread.wait()
        self.assertTrue(self.success)
        self._delete_url('/a/q/1')

    def test_message_put_wait_cleanup(self):
        self.success = False
        self._put_url('/a/q/1?hide=1')
        thread = eventlet.spawn(self._message_wait)
        eventlet.spawn_after(1, self.backend.clean)
        thread.wait()
        self.assertTrue(self.success)
        self._delete_url('/a/q/1')

    def test_message_post_wait(self):
        self.success = False
        self._put_url('/a/q/1?hide=10')
        thread = eventlet.spawn(self._message_wait)
        eventlet.spawn_after(0.2, self._post_url, '/a/q/1?hide=0')
        thread.wait()
        self.assertTrue(self.success)
        self._delete_url('/a/q/1')

    def test_message_post_wait_queue(self):
        self.success = False
        self._put_url('/a/q/1?hide=10')
        thread = eventlet.spawn(self._message_wait)
        url = '/a/q?hide=0&match_hidden=true'
        eventlet.spawn_after(0.2, self._post_url, url, status=204)
        thread.wait()
        self.assertTrue(self.success)
        self._delete_url('/a/q/1')

    def message(self, id, ttl=0, hide=0, body=''):
        return dict(id=id, ttl=ttl, hide=hide, body=body)

    def assertMessages(self, first, second):
        self.assertEquals(len(first), len(second))
        for x in xrange(0, len(second)):
            self.assertEquals(first[x]['id'], second[x]['id'])
            self.assertAlmostEquals(first[x]['ttl'], second[x]['ttl'])
            self.assertAlmostEquals(first[x]['hide'], second[x]['hide'])
            self.assertEquals(first[x]['body'], second[x]['body'])

    def _delete_url(self, url, status=204, **kwargs):
        return self._url('DELETE', url, status=status, **kwargs)

    def _get_url(self, url, **kwargs):
        return self._url('GET', url, **kwargs)

    def _post_url(self, url, **kwargs):
        return self._url('POST', url, **kwargs)

    def _put_url(self, url, status=201, **kwargs):
        return self._url('PUT', url, status=status, **kwargs)

    def _url(self, method, url, body='', status=200):
        req = webob.Request.blank('/v1.0' + url, method=method, body=body)
        res = req.get_response(self.frontend)
        self.assertEquals(res.status_int, status)
        if status == 200:
            return json.loads(res.body)
        return None


class TestWSGISQLite(TestWSGIMemory):
    '''Unittests for the WSGI frontend to SQLite backend.'''
    backend_class = burrow.backend.sqlite.Backend
