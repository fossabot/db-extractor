"""
setup - ensures proper package setup

This file is ensuring proper package setup is performed to ensure all prerequisites are satisfied 
and correct execution is possible
"""
# package to handle files/folders and related metadata/operations
import os
# facilitate dependencies management
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r') as fh:
    long_description_readme = fh.read()

this_package_website = 'https://github.com/danielgp/db-extractor'

setup(
    author='Daniel Popiniuc',
    author_email='danielpopiniuc@gmail.com',
    classifiers=[

        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: SQL',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    description='Extract information from databases to files, '
                'multiple formats supported, from a various SQL based servers',
    include_package_data=True,
    install_requires=[
        'Babel>=2.8,<3.0',
        'codetiming>=1.1,<2.0',
        'datedelta>=1.3,<2',
        'hdbcli>=2,<3',
        'mysql-connector-python>=8.0.11,<8.1',
        'pandas>=1,<2',
        'pyarrow>=1,<=5',
        'twine>3,<4',
        'xlrd3>=1,<=2',
        'XlsxWriter>=1,<=4',
        'wheel>=0.34.2,<1'
    ],
    keywords=[
        'csv'
    ],
    license='LGPL3',
    long_description=long_description_readme,
    long_description_content_type='text/markdown',
    name='db-extractor',
    packages=find_packages('db_extractor'),
    package_data={
        'db_extractor': [
            '*.json'
        ]
    },
    project_urls={
        'Documentation': this_package_website + '/blob/master/README.md',
        'Issue Tracker': this_package_website +
                         '/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc',
        'Source Code': this_package_website
    },
    python_requires='>=3.6',
    url=this_package_website + '/tags',  # project home page, if any
    version='1.2.4',
)
