#!/usr/bin/env python
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

'''Insert work to be done for a worker.'''
from __future__ import print_function

import sys

import burrow


body = None
if len(sys.argv) == 2:
    body = sys.stdin.read()
elif len(sys.argv) == 3:
    body = sys.argv[2]
else:
    print('Usage: %s <message id> [message body]' % sys.argv[0])
    sys.exit(1)

queue = burrow.Queue('test_account', 'test_queue')
queue.create_message(sys.argv[1], body)
