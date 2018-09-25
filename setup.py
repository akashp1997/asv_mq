#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

if (sys.version_info.major!=3):
    raise SystemError("Python version 2 is installed. Please use Python 3.")

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = open("README.md").read()

import asvmq
version = asvmq.__version__

setup(
    name="asvmq",
    version=version,
    description="ASV Messaging Queue API for message passing between processes.",
    long_description=readme,
    long_description_type="text/markdown",
    author="Akash Purandare",
    author_email="akash.p1997@gmail.com",
    url="https://github.com/akashp1997/asv_mq",
    packages=["asvmq"],
    include_package_data=True,
    install_requires=["pika>=0.12.0"],
    license="BSD-3-Clause",
    zip_safe=True,
    keywords="asv_mq"
)
