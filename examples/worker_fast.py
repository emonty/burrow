#!/usr/bin/env python

'''Fast worker example. This does an atomic get and delete on messages
to be processed, so if the message processing fails the work is lost.'''

import burrow


def process_messages(queue):
    while True:
        try:
            filters = dict(detail='all', wait=10)
            messages = queue.delete_messages(filters=filters)
            for message in messages:
                # Process message here
                print message
        except burrow.NotFound:
            continue


queue = burrow.Queue('test_account', 'test_queue')
try:
    process_messages(queue)
except KeyboardInterrupt:
    pass
