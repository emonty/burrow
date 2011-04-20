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

'''Configuration module for burrow.'''

import ConfigParser
import logging.config


class Config(object):
    '''Configuration class that wraps the ConfigParser get*
    methods. These wrappers automatically check for options in
    a specific instance section first before the regular section
    (section:instance and then section). They will also return a
    default value if given instead of throwing an exception.'''

    def __init__(self, config, section, instance=None):
        '''Initialize the config wrapper for section with an optional
        instance.'''
        self.config = config
        self.section = section
        if instance is None:
            self.instance = None
        else:
            self.instance = '%s:%s' % (section, instance)

    def get(self, option, default=None):
        '''Get the string value for an option, or the default value.'''
        return self._get(self.config.get, option, default)

    def getboolean(self, option, default=None):
        '''Get the boolean value for an option, or the default value.'''
        return self._get(self.config.getboolean, option, default)

    def getfloat(self, option, default=None):
        '''Get the float value for an option, or the default value.'''
        return self._get(self.config.getfloat, option, default)

    def getint(self, option, default=None):
        '''Get the integer value for an option, or the default value.'''
        return self._get(self.config.getint, option, default)

    def _get(self, method, option, default):
        '''Perform the get call, looking in instance, regular, and
        default sections before falling back to the default value.'''
        if self.instance is not None:
            if self.config.has_option(self.instance, option):
                return method(self.instance, option)
        if self.config.has_option(self.section, option):
            return method(self.section, option)
        if self.config.has_option(ConfigParser.DEFAULTSECT, option):
            return method(ConfigParser.DEFAULTSECT, option)
        return default


def load_config_files(config_files):
    '''Load the config files, if any, into the logging and ConfigParser
    modules.'''
    config = ConfigParser.ConfigParser()
    if config_files is not None:
        if len(config_files) > 0:
            logging.config.fileConfig(config_files)
        config.read(config_files)
    return config
