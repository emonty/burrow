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

'''Fast worker example. This does an atomic get and delete on messages
to be processed, so if the message processing fails the work is lost.'''
from __future__ import print_function

import burrow


def process_messages(queue):
    while True:
        try:
            filters = dict(detail='all', wait=10)
            messages = queue.delete_messages(filters=filters)
            for message in messages:
                # Process message here
                print(message)
        except burrow.NotFound:
            continue


queue = burrow.Queue('test_account', 'test_queue')
try:
    process_messages(queue)
except KeyboardInterrupt:
    pass
