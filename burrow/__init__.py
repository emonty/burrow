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

'''Main module for burrow package. This installs the gettext function
for all sub modules and packages.'''

import gettext

from burrow.client import Client, Account, Queue
from burrow.server import Server
from burrow.backend import NotFound, InvalidArguments

__version__ = '2011.2'

# This installs the _(...) function as a built-in so all other modules
# don't need to.
gettext.install('burrow')
