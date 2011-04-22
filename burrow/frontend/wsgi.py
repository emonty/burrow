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

'''WSGI frontend for the burrow server.'''

import json
import time

import eventlet
import eventlet.wsgi
import routes
import routes.middleware
import webob.dec
import webob.exc

import burrow.frontend

# Default configuration values for this module.
DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8080
DEFAULT_BACKLOG = 64
DEFAULT_SSL = False
DEFAULT_SSL_CERTFILE = 'example.pem'
DEFAULT_SSL_KEYFILE = 'example.key'
DEFAULT_THREAD_POOL_SIZE = 0
DEFAULT_TTL = 600
DEFAULT_HIDE = 0


def wait_on_queue(method):
    '''Decorator to wait on an account/queue if the wait option is
    given. This will block until a message in the queue is ready or
    the timeout expires.'''
    def wrapper(self, req, account, queue, *args, **kwargs):
        '''Wrapper method for wait_on_queue.'''
        wait = 0
        if 'wait' in req.params:
            wait = int(req.params['wait'])
            if wait > 0:
                wait += time.time()
        while True:
            res = method(self, req, account, queue, *args, **kwargs)
            if wait == 0 or res.status_int != 404:
                break
            now = time.time()
            if wait - now > 0:
                self.backend.wait(account, queue, wait - now)
            if wait < time.time():
                break
        return res
    return wrapper


class Frontend(burrow.frontend.Frontend):
    '''Frontend implementation that implements the Burrow v1.0 protocol
    using WSGI.'''

    def __init__(self, config, backend):
        super(Frontend, self).__init__(config, backend)
        self.default_ttl = self.config.get('default_ttl', DEFAULT_TTL)
        self.default_hide = self.config.get('default_hide', DEFAULT_HIDE)
        mapper = routes.Mapper()
        mapper.connect('/', action='root')
        mapper.connect('/v1.0', action='version')
        mapper.connect('/v1.0/{account}', action='account')
        mapper.connect('/v1.0/{account}/{queue}', action='queue')
        mapper.connect('/v1.0/{account}/{queue}/{message}', action='message')
        self._routes = routes.middleware.RoutesMiddleware(self._route, mapper)

    def run(self, thread_pool):
        '''Create the listening socket and start the thread that runs
        the WSGI server. This extra thread is needed since the WSGI
        server function blocks.'''
        host = self.config.get('host', DEFAULT_HOST)
        port = self.config.getint('port', DEFAULT_PORT)
        backlog = self.config.getint('backlog', DEFAULT_BACKLOG)
        socket = eventlet.listen((host, port), backlog=backlog)
        self.log.info(_('Listening on %s:%d') % (host, port))
        if self.config.getboolean('ssl', DEFAULT_SSL):
            certfile = self.config.get('ssl_certfile', DEFAULT_SSL_CERTFILE)
            keyfile = self.config.get('ssl_keyfile', DEFAULT_SSL_KEYFILE)
            socket = eventlet.green.ssl.wrap_socket(socket, certfile=certfile,
                keyfile=keyfile)
        thread_pool.spawn_n(self._run, socket, thread_pool)

    def _run(self, socket, thread_pool):
        '''Thread to run the WSGI server.'''
        thread_pool_size = self.config.getint('thread_pool_size',
            DEFAULT_THREAD_POOL_SIZE)
        log_format = '%(client_ip)s "%(request_line)s" %(status_code)s ' \
                     '%(body_length)s %(wall_seconds).6f'
        if thread_pool_size == 0:
            eventlet.wsgi.server(socket, self, log=WSGILog(self.log),
                log_format=log_format, custom_pool=thread_pool)
        else:
            eventlet.wsgi.server(socket, self, log=WSGILog(self.log),
                log_format=log_format, max_size=thread_pool_size)

    def __call__(self, *args, **kwargs):
        return self._routes(*args, **kwargs)

    @webob.dec.wsgify
    def _route(self, req):
        args = req.environ['wsgiorg.routing_args'][1]
        if not args:
            return webob.exc.HTTPNotFound()
        action = args.pop('action')
        method = getattr(self, '_%s_%s' % (req.method.lower(), action), False)
        if not method:
            return webob.exc.HTTPBadRequest()
        return method(req, **args)

    @webob.dec.wsgify
    def _get_root(self, _req):
        return webob.exc.HTTPOk(body=json.dumps(['v1.0'], indent=2))

    @webob.dec.wsgify
    def _delete_version(self, req):
        filters = self._parse_filters(req)
        self.backend.delete_accounts(filters)
        return webob.exc.HTTPNoContent()

    @webob.dec.wsgify
    def _get_version(self, req):
        filters = self._parse_filters(req)
        accounts = [account for account in self.backend.get_accounts(filters)]
        if len(accounts) == 0:
            return webob.exc.HTTPNotFound()
        return webob.exc.HTTPOk(body=json.dumps(accounts, indent=2))

    @webob.dec.wsgify
    def _delete_account(self, req, account):
        filters = self._parse_filters(req)
        self.backend.delete_queues(account, filters)
        return webob.exc.HTTPNoContent()

    @webob.dec.wsgify
    def _get_account(self, req, account):
        filters = self._parse_filters(req)
        queues = [queue for queue in self.backend.get_queues(account, filters)]
        if len(queues) == 0:
            return webob.exc.HTTPNotFound()
        return webob.exc.HTTPOk(body=json.dumps(queues, indent=2))

    @webob.dec.wsgify
    @wait_on_queue
    def _delete_queue(self, req, account, queue):
        filters = self._parse_filters(req)
        messages = [message for message in
            self.backend.delete_messages(account, queue, filters)]
        return self._return_messages(req, account, queue, messages, 'none')

    @webob.dec.wsgify
    @wait_on_queue
    def _get_queue(self, req, account, queue):
        filters = self._parse_filters(req)
        messages = [message for message in
            self.backend.get_messages(account, queue, filters)]
        return self._return_messages(req, account, queue, messages, 'all')

    @webob.dec.wsgify
    @wait_on_queue
    def _post_queue(self, req, account, queue):
        attributes = self._parse_attributes(req)
        filters = self._parse_filters(req)
        messages = [message for message in
            self.backend.update_messages(account, queue, attributes, filters)]
        return self._return_messages(req, account, queue, messages, 'all')

    @webob.dec.wsgify
    def _delete_message(self, req, account, queue, message):
        message = self.backend.delete_message(account, queue, message)
        if message is None:
            return webob.exc.HTTPNotFound()
        return self._return_message(req, account, queue, message, 'none')

    @webob.dec.wsgify
    def _get_message(self, req, account, queue, message):
        message = self.backend.get_message(account, queue, message)
        if message is None:
            return webob.exc.HTTPNotFound()
        return self._return_message(req, account, queue, message, 'all')

    @webob.dec.wsgify
    def _post_message(self, req, account, queue, message):
        attributes = self._parse_attributes(req)
        message = self.backend.update_message(account, queue, message,
            attributes)
        if message is None:
            return webob.exc.HTTPNotFound()
        return self._return_message(req, account, queue, message, 'id')

    @webob.dec.wsgify
    def _put_message(self, req, account, queue, message):
        attributes = self._parse_attributes(req, self.default_ttl,
            self.default_hide)
        if self.backend.create_message(account, queue, message, req.body,
            attributes):
            return webob.exc.HTTPCreated()
        return webob.exc.HTTPNoContent()

    def _filter_message(self, detail, message):
        if detail == 'id':
            return dict(id=message['id'])
        elif detail == 'attributes':
            message = message.copy()
            del message['body']
            return message
        elif detail == 'all':
            return message
        return None

    def _return_message(self, req, account, queue, message, detail):
        if 'detail' in req.params:
            detail = req.params['detail']
        if detail == 'body':
            return webob.exc.HTTPOk(body=message['body'])
        message = self._filter_message(detail, message)
        if message is not None:
            return webob.exc.HTTPOk(body=json.dumps(message, indent=2))
        return webob.exc.HTTPNoContent()

    def _return_messages(self, req, account, queue, messages, detail):
        if len(messages) == 0:
            return webob.exc.HTTPNotFound()
        if 'detail' in req.params:
            detail = req.params['detail']
        filtered_messages = []
        for message in messages:
            message = self._filter_message(detail, message)
            if message is not None:
                filtered_messages.append(message)
        if len(filtered_messages) == 0:
            return webob.exc.HTTPNoContent()
        return webob.exc.HTTPOk(body=json.dumps(filtered_messages, indent=2))

    def _parse_filters(self, req):
        filters = {}
        if 'limit' in req.params:
            filters['limit'] = int(req.params['limit'])
        if 'marker' in req.params:
            filters['marker'] = req.params['marker']
        if 'hidden' in req.params and req.params['hidden'].lower() == 'true':
            filters['match_hidden'] = True
        return filters

    def _parse_attributes(self, req, default_ttl=None, default_hide=None):
        attributes = {}
        if 'ttl' in req.params:
            ttl = int(req.params['ttl'])
        else:
            ttl = default_ttl
        if ttl is not None and ttl > 0:
            ttl += int(time.time())
        attributes['ttl'] = ttl
        if 'hide' in req.params:
            hide = int(req.params['hide'])
        else:
            hide = default_hide
        if hide is not None and hide > 0:
            hide += int(time.time())
        attributes['hide'] = hide
        return attributes


class WSGILog(object):
    '''Class for eventlet.wsgi.server to forward logging messages.'''

    def __init__(self, log):
        self.log = log

    def write(self, message):
        self.log.debug(message.rstrip())
