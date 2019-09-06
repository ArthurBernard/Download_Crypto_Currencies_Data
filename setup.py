#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-01-30 11:20:11
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-05 08:48:08

""" Setup file. """

# Built-in packages
from setuptools import setup, find_packages

# Local packages
import dccd

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='dccd',
    version=dccd.__version__,
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
