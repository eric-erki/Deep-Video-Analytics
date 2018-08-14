#!/usr/bin/env python

from setuptools import setup

setup(name='dvaclient',
      version='1.0',
      description='Deep Video Analytics Client',
      author='Akshay Bhat',
      author_email='dvaclient@deepvideoanalytics.com',
      url='https://www.deepvideoanalytics.com/',
      packages=['dvaclient'],
      package_data={'dvaclient': ['schema.json']},
      include_package_data=True,
      install_requires=[
            'jsonschema==2.6.0',
            'requests'
      ],
      )
