#!/usr/bin/env python

import re
from setuptools import setup

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'termcolor', 'Cython>=0.29.23', 'numpy>=1.21.0', 'scipy>=1.8.1',
    'argparse', 'GDAL>=3.0', 'rasterio>=1.1.5', 'Shapely==1.8.5.post1',
    'rs-commons'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('anthro/__version__.py').read(),
    re.M
).group(1)

setup(name='anthro',
      version=version,
      description='Riverscapes Open Source Python Anthropogenic Context',
      author='Jordan Gilbert',
      license='MIT',
      python_requires='>3.9.0',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      entry_points={
          "console_scripts": ['anthro = anthro.anthro:main']
      },
      zip_safe=False,
      url='https://github.com/Riverscapes/riverscapes-tools/tree/master/packages/anthro',
      packages=[
          'anthro'
      ]
      )
