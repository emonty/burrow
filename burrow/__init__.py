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

'''Main module for burrow package. This installs the gettext function
for all sub modules and packages.'''

import pbr

from burrow import client
from burrow import server

__version_info__ = pbr.version.VersionInfo("burrow")
__version__ = __version_info__.version_string()

Client = client.Client
Account = client.Account
Queue = client.Queue
Server = server.Server


class NotFound(Exception):
    '''Raised when an account, queue, or message can not be found.'''
    pass


class InvalidArguments(Exception):
    '''Raised when the given arguments are invalid, usually from attributes
    or filters.'''
    pass
