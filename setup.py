#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" setup.py for python-schema_registry."""

from setuptools import setup, find_packages

__version__ = "1.2.2"

with open("README.md") as readme_file:
    long_description = readme_file.read()

requires = ["fastavro<=0.22.3", "requests<=2.22.0"]

setup(
    name="python-schema-registry-client",
    version=__version__,
    description="Python Rest Client to interact against Schema Registry Confluent Server to manage Avro Schemas",
    long_description=long_description,
    author="Marcos Schroh",
    author_email="schrohm@gmail.com",
    install_requires=requires,
    extras_require={"faust": ["faust<=1.8.1"]},
    url="https://github.com/marcosschroh/python-schema-registry-client",
    download_url="",
    packages=find_packages(exclude=("tests",)),
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
