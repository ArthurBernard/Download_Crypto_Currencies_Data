#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

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
    long_description=str(long_description),
    install_requires=[
        'numpy>=1.14.1', 
        'pandas>=0.22.0', 
        'requests>=2.18.4', 
        'xlrd>=1.1.0', 
        'xlsxwriter>=1.0.2'
    ],
    url='https://github.com/ArthurBernard/Download_Crypto_Currencies_Data',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Financial and Insurance Industry',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Office/Business :: Financial',
    ],
)