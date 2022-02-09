#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" setup.py for python-schema-registry-client."""

from setuptools import find_packages, setup

__version__ = "2.2.2"

with open("README.md") as readme_file:
    long_description = readme_file.read()


requires = [
    "fastavro>=1.4.4",
    "jsonschema>=3.2.0",
    "httpx>=0.19.0",
    "aiofiles>=0.7.0",
]

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
        "faust": [
            "faust-streaming",
        ],
        "docs": [
            "mkdocs",
            "mkdocs-material",
        ],
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
            "pydantic",
        ],
    },
    url="https://github.com/marcosschroh/python-schema-registry-client",
    download_url="https://pypi.org/project/python-schema-registry-client/#files",
    packages=find_packages(
        exclude=(
            "tests",
            "docs",
        )
    ),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development",
    ],
    keywords=(
        """
        Schema Registry, Python, Avro, Apache, Apache Avro, JSON, JSON Schema
        """
    ),
)
