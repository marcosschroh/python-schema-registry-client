#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" setup.py for python-schemaregistry."""

from setuptools import setup, find_packages

__version__ = "0.0.1"

with open("README.md") as readme_file:
    long_description = readme_file.read()

requires = [
    "avro-python3",
    "fastavro",
    "faust==1.5.4",
    "requests==2.21.0",
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
    packages=find_packages(),
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