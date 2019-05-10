#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" setup.py for python-schemaregistry."""

from setuptools import setup

__version__ = "0.0.1"

with open("README.md") as readme_file:
    long_description = readme_file.read()

requires = [
    "avro-python3",
    "colorlog==3.1.4",
    "fastavro",
    "faust==1.5.4",
    "robinhood-aiokafka==1.0.2",
    "requests-async==0.4.1",
    "simple-settings==0.16.0",
]

setup(
    name="python-schemaregistry-client",
    version=__version__,
    description="A python client for schema registry.",
    long_description=long_description,
    author="Marcos Schroh",
    author_email="schrohm@gmail.com",
    install_requires=requires,
    url="https://github.com/marcosschroh/python-schemaregistry-client",
    download_url="",
    packages=[],
    include_package_data=True,
    license="GPLv3",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development",
    ],
    keywords=(
        """
        Schema Registry, Python, Avro, Apache, Apache Avro
        """
    ),
)