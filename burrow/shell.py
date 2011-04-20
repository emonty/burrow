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
    '''Shell session class.'''

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
        (self.options, self.args) = self.parser.parse_args()
        if self.options.commands:
            self.print_help()
            sys.exit(1)
        if self.options.files is None:
            files = []
        else:
            files = self.options.files.split(', ')
        self.client = burrow.Client(url=self.options.url, config_files=files)

    def run(self):
        '''Run the command given in arguments or enter an interactive shell.'''
        if len(self.args) == 0:
            for command in self._get_command():
                self.run_command(command[0], command[1:])
        else:
            self.run_command(self.args[0], self.args[1:])

    def _get_command(self):
        '''Get a command from stdin, printing a prompt out if stdin
        is attached to a TTY.'''
        prompt = ''
        if os.isatty(sys.stdin.fileno()):
            prompt = 'burrow> '
        try:
            # Try importing readline to make raw_input functionality more
            # user friendly.
            import readline
        except ImportError:
            pass
        while True:
            try:
                command = raw_input(prompt)
            except EOFError:
                if os.isatty(sys.stdin.fileno()):
                    print
                break
            command = command.split()
            if len(command) == 0:
                continue
            if command[0] == 'help':
                self.print_help(print_options_help=False)
                continue
            if command[0] == 'exit' or command[0] == 'quit':
                break
            yield command

    def run_command(self, command, args):
        '''Try running a command with the given arguments.'''
        section = self._get_section(command)
        if section is None:
            print _('Command not found: %s') % command
            return
        if len(args) != len(section['args']):
            for arg in section['args']:
                command += ' <%s>' % arg
            print _('Wrong number of arguments: %s') % command
            return
        if section.get('account', None):
            args.insert(0, self.options.account)
        if command in self.stdin_commands:
            args.append(sys.stdin.read())
        if command in self.attribute_commands:
            args.append(self._pack_attributes())
        if section.get('filters', None):
            args.append(self._pack_filters())
        try:
            result = getattr(self.client, command)(*args)
        except Exception, exception:
            print exception
            return
        self._print_result(result)

    def _get_section(self, command):
        '''Lookup command in the defined command sections.'''
        for section in self.sections:
            if command in section['commands']:
                return section
        return None

    def _pack_attributes(self):
        '''Pack attributes given in command line options.'''
        attributes = {}
        if self.options.ttl is not None:
            attributes['ttl'] = self.options.ttl
        if self.options.hide is not None:
            attributes['hide'] = self.options.hide
        return attributes

    def _pack_filters(self):
        '''Pack filters given in command line options.'''
        filters = {}
        if self.options.limit is not None:
            filters['limit'] = self.options.limit
        if self.options.marker is not None:
            filters['marker'] = self.options.marker
        if self.options.all is not None:
            filters['match_hidden'] = self.options.all
        return filters

    def _print_result(self, result):
        '''Format and print the result.'''
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    self._print_message(item)
                else:
                    print item
        elif isinstance(result, dict):
            self._print_message(result)
        elif result is not None:
            print result

    def _print_message(self, item):
        '''Format and print message.'''
        print 'id =', item['id']
        for key, value in item.iteritems():
            if key != 'id':
                print '    ', key, '=', value

    def print_help(self, print_options_help=True):
        '''Print the parser generated help along with burrow command help.'''
        if print_options_help:
            self.parser.print_help()
            print
        for section in self.sections:
            print '%s commands:' % section['name']
            for command in section['commands']:
                help_string = ''
                if section.get('filters', None):
                    help_string += ' [filters]'
                if command in self.attribute_commands:
                    help_string += ' [attributes]'
                for arg in section['args']:
                    help_string += ' <%s>' % arg
                print '    %s%s' % (command, help_string)
            print


if __name__ == '__main__':
    Shell().run()
