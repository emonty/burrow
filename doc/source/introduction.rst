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

Introduction to Message Queues
******************************

Message queues can be utilized by applications for a number of reasons,
but at the core they are used to enable asynchronous communication
between different threads of execution. Messages may be passed between
threads, process, machines, or data centers. At a higher level, they
are usually used to distributed work to other threads or programs that
are more suitable or even specialized. For example, queue consumers
may send email, resize images, or start virtual machines on behalf
of the caller. For a more general introduction to message queues,
see the Wikipedia page on `Message Queues`_.

.. _`Message Queues`: http://en.wikipedia.org/wiki/Message_queue

Message queue solutions are usually designed with a specific scope or
environment in mind. For example, the Python :mod:`Queue` module is
designed to be used within the same process. `RabbitMQ`_ is primarily
designed for clustered applications. The protocols used for network
based queues play a role in this as well, for example previous
versions of `AMQP`_ (pre 1.0) were not well suited for high-latency
environments, but `AMQP`_ 1.0 allows pipelining to overcome this
limitation. Some message queue solutions involve a broker to route
messages between producers and consumers, where others such as
`ZeroMQ`_ allow direct message passing. Some message queues such as
`Gearman`_ are designed for a single tenant operation, where others
like `RabbitMQ`_ allow for multiple accounts.

.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`AMQP`: http://www.amqp.org/
.. _`ZeroMQ`: http://www.zeromq.org/
.. _`Gearman`: http://gearman.org/

Burrow is slightly different in that the focus is on distributed
messaging (between data centers) so it's appropriate for both low
and high-latency environments. It is also designed to handle large
numbers of tenants in order to be suitable for a large public cloud
deployments. Beyond being a simple message queue, Burrow also provides
a fanout event notification mechanism so a single message may be read
by multiple readers. Readers can ask for new messages with a marker
to maintain the state of the last seen message.
