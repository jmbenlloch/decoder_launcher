#! /usr/bin/env python

# Author: P. Novella

"""
Setup script for shiftertools distribution.
"""

from distutils.core import setup
import os

long_description = """\
Collection of tools for NEXT shifters
"""

packages = ['shiftertools']

setup (
    name='shiftertools',
    version="1.0.0",
    description='NEXT shifter tools',
    long_description=long_description,
    author='Pau Novella',
    author_email='pau.novella@ific.uv.es',
    url='http://next.ific.uv.es',
    license='NEXT',
    scripts=['scripts/runFileDeco',
             'scripts/runDecoDaemon',
             'scripts/checkDecoFiles'],
    packages=packages
    )
