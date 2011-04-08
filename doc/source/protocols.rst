..
  Copyright (C) 2011 OpenStack LLC.
 
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

Protocols
*********

REST (HTTP) v1.0
================

The REST protocol is currently implemented by the
:mod:`burrow.frontend.wsgi` module. Other modules may implement
this protocol in the future, other versions of the protocol may be
developed, and entirely new REST protocols may be developed. The
protocol is versioned by using the first part of the path so changes
can be introduced while maintaining backwards compatibility.

Operations
----------

The supported HTTP methods and URLs are:

============================== =============================================
URL                            Description
============================== =============================================
**GET**
----------------------------------------------------------------------------
/                              List all supported versions.
/version                       List all accounts that have messages in them.
/version/account               List all queues that have message in them.
/version/account/queue         List all messages in the queue.
/version/account/queue/message List the message with the given id.
**PUT**
----------------------------------------------------------------------------
/version/account/queue/message Insert a message with the given message id,
                               overwriting a previous message with the
                               id if one existed.
**POST**
----------------------------------------------------------------------------
/version/account/queue         Update the attributes for all messages in the
                               queue.
/version/account/queue/message Update the attributes for the message with
                               the given id.
**DELETE**
----------------------------------------------------------------------------
/version/account               Remove all messages in the account.
/version/account/queue         Remove all messages in the queue.
/version/account/queue/message Remove the message with the given id. 
============================== =============================================

The GET, POST, and DELETE methods for root, accounts, and queues
have the ability to filter messages using the parameters listed in
:keyword:`filters`. Message attributes and filters are specified
using URL parameters such as ``GET /version/account?limit=5``.
