#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-01-30 11:20:11
# @Last modified by: ArthurBernard
# @Last modified time: 2019-11-07 16:58:33

""" Setup file. """

# Built-in packages
from setuptools import setup, find_packages

# Local packages


MAJOR = 1
MINOR = 1
PATCH = 1
ISRELEASED = True
VERSION = '{}.{}.{}'.format(MAJOR, MINOR, PATCH)


def get_version_info():
    FULLVERSION = VERSION
    GIT_REVISION = ""

    if not ISRELEASED:
        FULLVERSION += '.dev' + GIT_REVISION[:7]

    return FULLVERSION, GIT_REVISION


def write_version_py(filename='dccd/version.py'):
    cnt = """
# THIS FILE IS GENERATED FROM FYNANCE SETUP.PY
short_version = '%(version)s'
version = '%(version)s'
full_version = '%(full_version)s'
git_revision = '%(git_revision)s'
release = %(isrelease)s
if not release:
    version = full_version
"""
    FULLVERSION, GIT_REVISION = get_version_info()

    a = open(filename, 'w')
    try:
        a.write(cnt % {'version': VERSION,
                       'full_version': FULLVERSION,
                       'git_revision': GIT_REVISION,
                       'isrelease': str(ISRELEASED)})
    finally:
        a.close()


write_version_py()

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='dccd',
    version=VERSION,
    packages=find_packages(),
    author='Arthur Bernard',
    author_email='arthur.bernard.92@gmail.com',
    description='Download Crypto Currenciy Data from different exchanges.',
    license='MIT',
    long_description=str(long_description),
    install_requires=[
        'numpy>=1.14.1',
        'pandas>=0.22.0',
        'requests>=2.18.4',
        'xlrd>=1.1.0',
        'xlsxwriter>=1.0.2',
        'websockets>=7.0.0',
        'scipy>=1.2.0',
        'SQLAlchemy>=1.3.0',
    ],
    url='https://github.com/ArthurBernard/Download_Crypto_Currencies_Data',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Office/Business :: Financial',
    ],
)
