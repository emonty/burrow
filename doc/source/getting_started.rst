..
  Copyright (C) 2011 OpenStack Foundation
 
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

Getting Started
***************

System Requirements
===================

Burrow development currently targets Ubuntu Server 10.04, but should
work on most Linux platforms with the following software:

* Python 2.6
* Eventlet 0.9.8
* WebOb 0.9.8
* Routes
* Setuptools
* Nose
* Sphinx

Getting Burrow
==============

Burrow's source code is hosted on Launchpad and managed with Bazaar.
The current trunk can be checked out with its Launchpad alias:

    ``bzr branch lp:burrow``

A source tarball for the latest release of Burrow is available on the
`Launchpad project page <https://launchpad.net/burrow>`_.

Prebuilt packages for Ubuntu are not yet available.

Development
===========

To get started with Burrow development (and
other OpenStack projects), see the `How To Contribute
<http://wiki.openstack.org/HowToContribute>`_ page on the OpenStack
wiki. You may also want to look at the `blueprints for Burrow
<https://blueprints.launchpad.net/burrow>`_ or ask about specific
features on the OpenStack mailing list and IRC channel.

Production
==========

It is not yet recommended to use Burrow in production environments. It
should be production ready for small deployments with the next release,
and larger deployments the release after.

Running
=======

Once you have the required software installed and Burrow downloaded
(either by source tarball or bzr branch), you can either run it
directly from the source directory or install it. If you choose to
install, run:

    ``python setup.py install``

If you are running from the source directory, you will want to prepend
the ``burrow`` and ``burrowd`` commands with ``bin/``. To start the
server, run:

    ``burrowd``

This should start the default server modules and enter into the server
loop. Press CTRL-C to stop.

To use the command line client, run:

    ``burrow``

This should output fairly detailed help messages on the available
commands and options. To inset a message (make sure ``burrowd``
is still running), run:

    ``echo "data" | burrow create_message test_queue message_1``

This will insert a new message into the queue ``test_queue`` with
message ID ``mesage_1`` and message payload ``data``. To list the queues, run:

    ``burrow get_queues``

As expected, the queue ``test_queue`` should be returned. Note that
by default the ``burrow`` command line tool uses your login ID as
the account, so all queue and message operations are in the context
of this account ID. You can change the default account ID with the
``-a <account>`` option.

To list all messages in ``test_queue``, run:

    ``burrow get_messages test_queue``

To delete the message we previously created, run:

    ``burrow delete_message test_queue message_1``

From here you can create more messages, delete all messages in an
account or queue, or update one or more messages with the ``update_*``
commands.
