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
import types

import eventlet.wsgi
import routes.middleware
import webob.dec

import burrow.backend
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


class Frontend(burrow.frontend.Frontend):
    '''Frontend implementation that implements the Burrow v1.0 protocol
    using WSGI.'''

    def __init__(self, config, backend):
        super(Frontend, self).__init__(config, backend)
        self.default_ttl = int(self.config.get('default_ttl', DEFAULT_TTL))
        self.default_hide = int(self.config.get('default_hide', DEFAULT_HIDE))
        mapper = routes.Mapper()
        mapper.connect('/', action='versions')
        mapper.connect('/v1.0', action='accounts')
        mapper.connect('/v1.0/{account}', action='queues')
        mapper.connect('/v1.0/{account}/{queue}', action='messages')
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
            eventlet.wsgi.server(socket, self, log=_WSGILog(self.log),
                log_format=log_format, custom_pool=thread_pool)
        else:
            eventlet.wsgi.server(socket, self, log=_WSGILog(self.log),
                log_format=log_format, max_size=thread_pool_size)

    def __call__(self, *args, **kwargs):
        return self._routes(*args, **kwargs)

    @webob.dec.wsgify
    def _route(self, req):
        '''Parse the request args and see if there is a matching method.'''
        args = req.environ['wsgiorg.routing_args'][1]
        if not args:
            return self._response(status=404)
        action = args.pop('action')
        method = getattr(self, '_%s_%s' % (req.method.lower(), action), None)
        if method is not None:
            return method(req, **args)
        method = req.method.lower()
        args = dict(args)
        if method == 'post':
            method = 'update'
            args['attributes'] = self._parse_attributes(req)
        method = getattr(self.backend, '%s_%s' % (method, action), None)
        if method is None:
            return self._response(status=400)
        args['filters'] = self._parse_filters(req)
        return self._response(body=lambda: method(**args))

    @webob.dec.wsgify
    def _get_versions(self, _req):
        '''Return a list of API versions.'''
        return self._response(body=['v1.0'])

    @webob.dec.wsgify
    def _put_message(self, req, account, queue, message):
        '''Read the request body and create a new message.'''
        attributes = self._parse_attributes(req, self.default_ttl,
            self.default_hide)
        body = ''
        for chunk in iter(lambda: req.body_file.read(16384), ''):
            body += str(chunk)
        if self.backend.create_message(account, queue, message, body,
            attributes):
            return self._response(status=201)
        return self._response()

    def _parse_filters(self, req):
        '''Parse filters from a request object and build a dict to
        pass into the backend methods.'''
        filters = {}
        if 'limit' in req.params:
            filters['limit'] = int(req.params['limit'])
        if 'marker' in req.params:
            filters['marker'] = req.params['marker']
        if 'match_hidden' in req.params and \
            req.params['match_hidden'].lower() == 'true':
            filters['match_hidden'] = True
        if 'detail' in req.params:
            filters['detail'] = req.params['detail']
        if 'wait' in req.params:
            filters['wait'] = int(req.params['wait'])
        return filters

    def _parse_attributes(self, req, default_ttl=None, default_hide=None):
        '''Parse attributes from a request object and build a dict
        to pass into the backend methods.'''
        attributes = {}
        if 'ttl' in req.params:
            ttl = int(req.params['ttl'])
        else:
            ttl = default_ttl
        attributes['ttl'] = ttl
        if 'hide' in req.params:
            hide = int(req.params['hide'])
        else:
            hide = default_hide
        attributes['hide'] = hide
        return attributes

    def _response(self, status=200, body=None, content_type=None):
        '''Pack result into an appropriate HTTP response.'''
        status, body = self._response_body(status, body)
        if body is None:
            content_type = ''
            if status == 200:
                status = 204
        else:
            if content_type is None:
                if isinstance(body, list) or isinstance(body, dict):
                    content_type = 'application/json'
                else:
                    content_type = 'application/octet-stream'
            if content_type == 'application/json':
                body = json.dumps(body, indent=2)
        response = webob.Response(status=status)
        if body is not None:
            response.content_type = content_type
            if isinstance(body, unicode):
                response.unicode_body = body
            else:
                response.body = body
        return response

    def _response_body(self, status, body):
        '''Normalize the body from the type given.'''
        try:
            if isinstance(body, types.FunctionType):
                body = body()
            if isinstance(body, types.GeneratorType):
                body = list(body)
        except burrow.backend.InvalidArguments, exception:
            status = 400
            body = exception.message
        except burrow.backend.NotFound, exception:
            status = 404
            body = exception.message
        if body == []:
            body = None
        return status, body


class _WSGILog(object):
    '''Class for eventlet.wsgi.server to forward logging messages.'''

    def __init__(self, log):
        self.log = log

    def write(self, message):
        '''Write WSGI log message to burrow log.'''
        self.log.debug(message.rstrip())
