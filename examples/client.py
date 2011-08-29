#!/usr/bin/env python

'''Insert work to be done for a worker.'''

import sys

import burrow


body = None
if len(sys.argv) == 2:
    body = sys.stdin.read()
elif len(sys.argv) == 3:
    body = sys.argv[2]
else:
    print 'Usage: %s <message id> [message body]' % sys.argv[0]
    sys.exit(1)

queue = burrow.Queue('test_account', 'test_queue')
queue.create_message(sys.argv[1], body)
