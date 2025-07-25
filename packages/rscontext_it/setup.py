#!/usr/bin/env python

import re
from setuptools import setup

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'termcolor', 'Cython>=0.29.7', 'numpy>=1.16.3', 'scipy>=1.8',
    'argparse', 'GDAL>=3.0', 'rasterio>=1.1.5', 'Shapely==1.8.5.post1',
    'rs-commons'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('rscontextit/__version__.py', encoding='utf8').read(),
    re.M
).group(1)

setup(name='rs-context-it',
      version=version,
      description='Riverscapes Context IT',
      author='Lorin Gaertner',
      license='MIT',
      python_requires='>3.5.2',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      entry_points={
          "console_scripts": ['rscontextit = rscontextit.rs_context_it:main']
      },
      zip_safe=False,
      url='https://github.com/Riverscapes/rs-context',
      packages=[
          'rscontextit'
      ]
      )
