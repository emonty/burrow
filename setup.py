#!/usr/bin/python
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

from setuptools import setup, find_packages
from setuptools.command.sdist import sdist
import os
import subprocess
try:
    from babel.messages import frontend
except ImportError:
    frontend = None


class local_sdist(sdist):
    """Customized sdist hook - builds the ChangeLog file from VC first"""

    def run(self):
        if os.path.isdir('.bzr'):
            # We're in a bzr branch

            log_cmd = subprocess.Popen(["bzr", "log", "--gnu"],
                                       stdout=subprocess.PIPE)
            changelog = log_cmd.communicate()[0]
            with open("ChangeLog", "w") as changelog_file:
                changelog_file.write(changelog)
        sdist.run(self)


name = 'burrow'


cmdclass = {'sdist': local_sdist}


if frontend:
    cmdclass.update({
        'compile_catalog': frontend.compile_catalog,
        'extract_messages': frontend.extract_messages,
        'init_catalog': frontend.init_catalog,
        'update_catalog': frontend.update_catalog})


setup(
    name=name,
    version='0.1',
    description='Burrow',
    license='Apache License (2.0)',
    author='OpenStack, LLC.',
    author_email='openstack-admins@lists.launchpad.net',
    url='https://launchpad.net/burrow',
    packages=find_packages(exclude=['test', 'bin']),
    test_suite='nose.collector',
    cmdclass=cmdclass,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)'],
    scripts=[
        'bin/burrow',
        'bin/burrowd'])
