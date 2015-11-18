#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from disutils.core import setup

setup(
    name="quikql",
    version="0.0.1",
    author="Tim Konick",
    author_email="konick781@gmail.com",
    url="",
    description="Sqlite3 wrapper",
    long_description=open('README.md').read(),
    license=open('LICENSE').read(),
    packages=["quikql"],
    package_dir={"quikql":"quikql"}
)
