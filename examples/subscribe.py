#!/usr/bin/env python

'''Subscription worker example. This never deletes messages, it only gets
messages and keeps state of the last message seen.'''

import burrow


def process_messages(queue):
    marker = None
    while True:
        try:
            filters = dict(marker=marker, wait=10)
            messages = queue.get_messages(filters=filters)
            for message in messages:
                # Process message here
                print message
                marker = message['id']
        except burrow.NotFound:
            continue


queue = burrow.Queue('test_account', 'test_queue')
try:
    process_messages(queue)
except KeyboardInterrupt:
    pass
