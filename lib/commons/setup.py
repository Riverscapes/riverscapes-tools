#!/usr/bin/env python

from setuptools import setup
import re

# https://packaging.python.org/discussions/install-requires-vs-requirements/
install_requires = [
    'geojson', 'sciencebasepy', 'requests', 'semver>=2.10.2',
    'termcolor', 'Cython>=0.29.23', 'numpy>=1.21.0', 'scipy>=1.8.1',
    'argparse', 'GDAL>=3.0', 'rasterio>=1.1.5', 'Shapely==1.8.5.post1',
    'jinja2>=2.11.3', 'psutil==5.8.0'
]

with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

version = re.search(
    '^__version__\\s*=\\s*"(.*)"',
    open('rscommons/__version__.py').read(),
    re.M
).group(1)

setup(name='rs-commons',
      version=version,
      description='Riverscapes helpers for use across Python3 open-source GIS Stack',
      author='Matt Reimer',
      license='MIT',
      python_requires='>3.9.0',
      long_description=long_descr,
      author_email='info@northarrowresearch.com',
      install_requires=install_requires,
      zip_safe=False,
      url='https://github.com/Riverscapes/rs-commons-python',
      packages=[
          'rscommons',
          'rscommons.classes'
      ],
      )
