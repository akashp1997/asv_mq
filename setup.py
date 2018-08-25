#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

if (sys.version_info.major!=3):
    raise SystemError("Python version 2 is installed. Please use Python 3.")

if (sys.platform=="linux" or sys.platform=="linux2"):
    #Check for installation type
    pass

elif (sys.platform=="darwin"):
    #Brew installation
    brew_installed = 0==os.system("which brew")
    if (not brew_installed):
        print("Homebrew is not installed on your system, installing Homebrew.")
        brew_install = 0==os.system("/usr/bin/ruby -e '$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)'")
        if(not brew_install):
            raise SystemError("Cannot install Homebrew on your system.")

    rabbitmq_installed = 0==os.system("brew install rabbitmq")
    if(not rabbitmq_installed):
        raise SystemError("RabbitMQ could not be installed using Homebrew.")
else:
    raise SystemError("This package cannot be install on Windows or Cygwin.")

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name="asv_mq",
    version="0.0.1",
    description="ASV Messaging Queue API for message passing between processes.",
    long_description="ASVMQ for Message passing between processes using Protobuf and RabbitMQ",
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
