#!/usr/bin/env python
from setuptools import setup

with open('README.md', 'r') as fh:
    long_desc = fh.read()

setup(name='pipelinewise-tap-mongodb',
      version='1.1.0',
      description='Singer.io tap for extracting data from MongoDB - Pipelinewise compatible',
      long_description=long_desc,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-mongodb',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mongodb'],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'pymongo==3.10.*',
          'tzlocal==2.0.*',
          'terminaltables==3.1.*',
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb'
          ],
          'test': [
              'pytest==5.4',
              'pytest-cov==2.10'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mongodb=tap_mongodb:main
      ''',
      packages=['tap_mongodb', 'tap_mongodb.sync_strategies'],
)
