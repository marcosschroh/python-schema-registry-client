#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" setup.py for python-schema-registry-client."""

from setuptools import find_packages, setup

__version__ = "1.7.2"

with open("README.md") as readme_file:
    long_description = readme_file.read()

requires = ["fastavro==1.0.0.post1", "httpx>=0.14,<0.15", "aiofiles>=0.5.0",]

description = "Python Rest Client to interact against Schema Registry Confluent Server to manage Avro Schemas"

setup(
    name="python-schema-registry-client",
    version=__version__,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Marcos Schroh",
    author_email="schrohm@gmail.com",
    install_requires=requires,
    extras_require={
        "faust": ["faust-streaming",],
        "docs": ["mkdocs", "mkdocs-material",],
        "tests": [
            "black",
            "autoflake",
            "flake8",
            "mypy==0.782",
            "isort",
            "pytest",
            "pytest-mock",
            "pytest-asyncio",
            "faker",
            "codecov",
            "pytest-cov",
            "dataclasses-avroschema",
        ],
    },
    url="https://github.com/marcosschroh/python-schema-registry-client",
    download_url="https://pypi.org/project/python-schema-registry-client/#files",
    packages=find_packages(exclude=("tests", "docs",)),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development",
    ],
    keywords=(
        """
        Schema Registry, Python, Avro, Apache, Apache Avro
        """
    ),
)
