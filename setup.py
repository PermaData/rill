#!/usr/bin/env python
import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand


from rill import __version__


class Tox(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['--recreate']
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(self.test_args)
        sys.exit(errno)


# Test dependencies
with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    tests_requires = f.read().split()


# Dependencies
install_requires = []

# Dependencies: Python 3.x backports for 2.x
if sys.version_info.major < 3:
    install_requires.append('enum34')  # enum.Enum


setup(
    name='rill',
    version=__version__,

    author='Chad Dombrova',
    author_email='chadrik@gmail.com',

    description='Flow Based Programming for Python',
    long_description=open('README.md').read(),

    url='https://github.com/chadrik/rill',
    packages=['rill', 'rill.components', 'rill.engine'],
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers'
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    cmdsclass={
        'test': Tox,
    },
    install_requires=install_requires,
    tests_require=tests_requires
)
