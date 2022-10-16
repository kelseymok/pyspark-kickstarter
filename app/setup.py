from glob import glob
from setuptools import setup, find_packages
from os.path import basename
from os.path import splitext

setup(
    name="transformer",
    version='0.1.0',
    description='Data Transformer for the coolest project ever.',
    author='Kelsey Mok',
    author_email='kelseymok@gmail.com',
    url='https://github.com/kelseymok/pyspark-kickstarter',
    packages=find_packages('src', exclude=('tests')),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
)
