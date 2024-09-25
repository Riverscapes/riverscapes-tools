from setuptools import setup
from Cython.Build import cythonize
import numpy

# setup.py
# Compile this script run: python setup.py build_ext --inplace
setup(
    ext_modules=cythonize("access.pyx"),
    include_dirs=[numpy.get_include()]
)
