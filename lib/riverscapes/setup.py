#!/usr/bin/env python

import re
from setuptools import setup

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'termcolor', 'Cython>=0.29.7', 'numpy>=1.16.3',
    'rsxml',
    'argparse'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('riverscapes/__version__.py', 'r', encoding='utf8').read(),
    re.M
).group(1)

setup(name='riverscapes',
      version=version,
      description='Riverscapes API',
      author='Matt Reimer',
      license='MIT',
      python_requires='>3.5.2',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      zip_safe=False,
      packages=[
          'riverscapes'
      ]
      )
