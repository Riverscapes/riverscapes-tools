#!/usr/bin/env python

from setuptools import setup
import re

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'termcolor', 'Cython>=0.29.7', 'numpy>=1.16.3', 'scipy>=1.5'
    'argparse', 'GDAL>=3.0', 'rasterio>=1.1.5', 'Shapely==1.8.2', 'scikit-fuzzy>=0.4.2',
    'rs-commons'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('sqlbrat/__version__.py').read(),
    re.M
).group(1)

setup(name='sqlbrat',
      version=version,
      description='Riverscapes Open Source Python BRAT',
      author='Matt Reimer',
      license='MIT',
      python_requires='>3.5.2',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      zip_safe=False,
      entry_points={
          "console_scripts": [
              'bratbuild = sqlbrat.brat_build:main',
              'bratrun = sqlbrat.brat_run:main'
          ]
      },
      url='https://github.com/Riverscapes/vbet',
      packages=[
          'sqlbrat'
      ]
      )
