#!/usr/bin/env python

'''Normal worker example. This first retrieves messages to be processed
and then deletes them once work is complete.'''

import burrow


def process_messages(queue):
    while True:
        try:
            messages = queue.get_messages(filters=dict(wait=10))
            for message in messages:
                # Process message here
                print message
                queue.delete_message(message['id'])
        except burrow.NotFound:
            continue


queue = burrow.Queue('test_account', 'test_queue')
try:
    process_messages(queue)
except KeyboardInterrupt:
    pass
