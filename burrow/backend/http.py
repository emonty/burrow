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

'''HTTP backend for burrow using httplib.'''

import httplib
import json
import urlparse

import burrow.backend

# Default configuration values for this module.
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080


class Backend(burrow.backend.Backend):
    '''This backend forwards all requests via HTTP using the httplib
    module. It is used for clients and proxies.'''

    def __init__(self, config):
        super(Backend, self).__init__(config)
        url = self.config.get('url')
        if url:
            url = urlparse.urlparse(url)
            self.config.set('host', url.hostname)
            if url.port is not None:
                self.config.set('port', str(url.port))
        host = self.config.get('host', DEFAULT_HOST)
        port = self.config.getint('port', DEFAULT_PORT)
        self.server = (host, port)

    def delete_accounts(self, filters={}):
        url = self._add_parameters('', filters=filters)
        return self._request('DELETE', url)

    def get_accounts(self, filters={}):
        url = self._add_parameters('', filters=filters)
        return self._request('GET', url)

    def delete_queues(self, account, filters={}):
        url = self._add_parameters('/%s' % account, filters=filters)
        return self._request('DELETE', url)

    def get_queues(self, account, filters={}):
        url = self._add_parameters('/%s' % account, filters=filters)
        return self._request('GET', url)

    def delete_messages(self, account, queue, filters={}):
        url = '/%s/%s' % (account, queue)
        url = self._add_parameters(url, filters=filters)
        return self._request('DELETE', url)

    def get_messages(self, account, queue, filters={}):
        url = '/%s/%s' % (account, queue)
        url = self._add_parameters(url, filters=filters)
        return self._request('GET', url)

    def update_messages(self, account, queue, attributes, filters={}):
        url = '/%s/%s' % (account, queue)
        url = self._add_parameters(url, attributes, filters)
        return self._request('POST', url)

    def create_message(self, account, queue, message, body, attributes={}):
        url = '/%s/%s/%s' % (account, queue, message)
        url = self._add_parameters(url, attributes)
        try:
            return self._request('PUT', url, body=body).next()
        except StopIteration:
            return False

    def delete_message(self, account, queue, message, filters={}):
        url = '/%s/%s/%s' % (account, queue, message)
        url = self._add_parameters(url, filters=filters)
        try:
            return self._request('DELETE', url).next()
        except StopIteration:
            return None

    def get_message(self, account, queue, message, filters={}):
        url = '/%s/%s/%s' % (account, queue, message)
        url = self._add_parameters(url, filters=filters)
        try:
            return self._request('GET', url).next()
        except StopIteration:
            return None

    def update_message(self, account, queue, message, attributes, filters={}):
        url = '/%s/%s/%s' % (account, queue, message)
        url = self._add_parameters(url, attributes, filters)
        try:
            return self._request('POST', url).next()
        except StopIteration:
            return None

    def clean(self):
        pass

    def _add_parameters(self, url, attributes={}, filters={}):
        separator = '?'
        for attribute in ['ttl', 'hide']:
            value = attributes.get(attribute, None)
            if value is not None:
                url += '%s%s=%s' % (separator, attribute, value)
                separator = '&'
        for filter in ['marker', 'limit', 'match_hidden', 'detail', 'wait']:
            value = filters.get(filter, None)
            if value is not None:
                url += '%s%s=%s' % (separator, filter, value)
                separator = '&'
        return url

    def _request(self, method, url, *args, **kwargs):
        connection = httplib.HTTPConnection(*self.server)
        connection.request(method, '/v1.0' + url, *args, **kwargs)
        response = connection.getresponse()
        if response.status >= 200 and response.status < 300:
            if int(response.getheader('content-length')) == 0:
                if response.status == 201:
                    yield True
                return
            body = response.read()
            if response.getheader('content-type')[:16] == 'application/json':
                body = json.loads(body)
                if isinstance(body, list):
                    for item in body:
                        yield item
                    return
            yield body
        if response.status == 400:
            raise burrow.backend.InvalidArguments(response.reason)
        if response.status == 404:
            raise burrow.backend.NotFound()
        raise Exception(response.reason)
