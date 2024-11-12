#!/usr/bin/env python

import re
from setuptools import setup

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'termcolor', 'argparse', 'GDAL>=3.0', 'rs-commons'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('ltpbrexport/__version__.py', encoding='utf8').read(),
    re.M
).group(1)

setup(name='ltpbrexport',
      version=version,
      description='LTPBR Exporter',
      author='Philip Bailey',
      license='MIT',
      python_requires='>3.9.0',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      entry_points={
          "console_scripts": ['ltpbrexport = ltpbrexport.ltpbrexport:main']
      },
      zip_safe=False,
      url='https://github.com/Riverscapes/ltpbrexport',
      packages=[
          'ltpbrexport'
      ]
      )
