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

'''
Burrow command line client and shell.
'''

import pwd
import optparse
import os
import sys

import burrow


class Shell(object):

    sections = [
        dict(name='Global',
            filters=True,
            args=[],
            commands=['delete_accounts', 'get_accounts']),
        dict(name='Account',
            account=True,
            filters=True,
            args=[],
            commands=['delete_queues', 'get_queues']),
        dict(name='Queue',
            account=True,
            filters=True,
            args=['queue'],
            commands=['delete_messages', 'get_messages', 'update_messages']),
        dict(name='Message',
            account=True,
            args=['queue', 'message'],
            commands=[
                'create_message',
                'delete_message',
                'get_message',
                'update_message'])]

    attribute_commands = [
        'update_messages',
        'create_message',
        'update_message']

    stdin_commands = ['create_message']

    def __init__(self):
        self.parser = optparse.OptionParser(usage='burrow [options] <command>',
            version=burrow.__version__)
        self.parser.add_option('-c', '--commands', action='store_true',
            help=_('Print help for the available commands'))
        self.parser.add_option('-u', '--url', default='http://localhost:8080',
            help=_('Backend URL to use'))
        rcfile = os.path.expanduser('~')
        rcfile = os.path.join(rcfile, '.burrowrc')
        if not os.path.exists(rcfile):
            rcfile = None
        self.parser.add_option('-f', '--files', default=rcfile,
            help=_('Configuration file(s) to use (comma separated)'))
        user = pwd.getpwuid(os.getuid())[0]
        self.parser.add_option('-a', '--account', default=user,
            help=_('Account to use for queue and message commands'))
        self.parser.add_option('-w', '--wait',
            help=_('Number of seconds to wait if no messages match'))

        attributes = optparse.OptionGroup(self.parser,
            _('Messages attribute options'))
        attributes.add_option('-t', '--ttl',
            help=_('TTL attribute in seconds to set for message(s)'))
        attributes.add_option('-H', '--hide',
            help=_('Hidden time attribute in seconds to set for message(s)'))
        self.parser.add_option_group(attributes)

        filters = optparse.OptionGroup(self.parser, _('Filtering options'))
        filters.add_option('-l', '--limit',
            help=_('Limit the number of messages to match'))
        filters.add_option('-m', '--marker',
            help=_('Only match messages that were inserted after this id'))
        filters.add_option('-A', '--all', action='store_true',
            help=_('Match all messages, including those that are hidden'))
        choices = ['none', 'id', 'attributes', 'all']
        filters.add_option('-d', '--detail', type='choice', choices=choices,
            help=_('What message information to return. Options are: %s') %
            ', '.join(choices))
        self.parser.add_option_group(filters)

    def run(self):
        (self.options, args) = self.parser.parse_args()
        if self.options.commands:
            self.print_help()
        if len(args) == 0:
            self.print_help(_('No command given'))
        if self.options.files is None:
            files = []
        else:
            files = self.options.files.split(', ')
        self.client = burrow.Client(url=self.options.url, config_files=files)
        for section in self.sections:
            if args[0] in section['commands']:
                self.run_command(section, args[0], args[1:])
        self.print_help(_('Command not found'))

    def run_command(self, section, command, args):
        if len(args) != len(section['args']):
            self.print_help(_('Wrong number of arguments'))
        if section.get('account', None):
            args.insert(0, self.options.account)
        if command in self.stdin_commands:
            args.append(sys.stdin.read())
        if command in self.attribute_commands:
            attributes = {}
            if self.options.ttl is not None:
                attributes['ttl'] = self.options.ttl
            if self.options.hide is not None:
                attributes['hide'] = self.options.hide
            args.append(attributes)
        if section.get('filters', None):
            filters = {}
            if self.options.limit is not None:
                filters['limit'] = self.options.limit
            if self.options.marker is not None:
                filters['marker'] = self.options.marker
            if self.options.all is not None:
                filters['match_hidden'] = self.options.all
            args.append(filters)
        getattr(self.client.backend, command)(*args)
        sys.exit(0)

    def print_help(self, message=None):
        if message:
            print message
            print
        self.parser.print_help()
        print
        for section in self.sections:
            print '%s commands:' % section['name']
            for command in section['commands']:
                help = ''
                if section.get('filters', None):
                    help += ' [filters]'
                if command in self.attribute_commands:
                    help += ' [attributes]'
                for arg in section['args']:
                    help += ' <%s>' % arg
                print '    %s%s' % (command, help)
            print
        sys.exit(1)


if __name__ == '__main__':
    Shell().run()
