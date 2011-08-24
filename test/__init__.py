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

'''Unittests for burrow.'''

import atexit
import os
import signal
import sys
import time

import burrow


def start_server():
    '''Fork and start the server, saving the pid in a file.'''
    kill_server()
    pid = os.fork()
    if pid == 0:
        try:
            import coverage
            cov = coverage.coverage(data_suffix=True)
            cov.start()

            def save_coverage(_signum, _frame):
                '''Callback for signal to save coverage info to file.'''
                cov.save()

            signal.signal(signal.SIGUSR1, save_coverage)
        except ImportError:
            pass
        server = burrow.Server(add_default_log_handler=False)
        server.frontends[0].default_ttl = 0
        server.run()
        sys.exit(0)
    pid_file = open('TestHTTP.pid', 'w')
    pid_file.write(str(pid))
    pid_file.close()
    atexit.register(kill_server)
    time.sleep(1)


def kill_server():
    '''Try killing the server if the pid file exists.'''
    try:
        pid_file = open('TestHTTP.pid', 'r')
        pid = pid_file.read()
        pid_file.close()
        try:
            os.kill(int(pid), signal.SIGUSR1)
            time.sleep(1)
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            pass
        os.unlink('TestHTTP.pid')
    except IOError:
        pass

start_server()
