#!/usr/bin/env python

from setuptools import setup
import re

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
    open('rvd/__version__.py').read(),
    re.M
).group(1)

setup(name='rvd',
      version=version,
      description='RVD',
      author='Philip Bailey',
      license='MIT',
      python_requires='>3.9.0',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      entry_points={
          "console_scripts": ['rvd = rvd.rvd:main']
      },
      zip_safe=False,
      url='https://github.com/Riverscapes/gnat',
      packages=[
          'rvd'
      ]
      )
