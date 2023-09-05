from setuptools import find_packages, setup

from analytics_db import __version__

setup(
    name='analytics_db',
    version=__version__,

    url='https://github.com/lemon-ai-com/analytics_db',
    author='Lemon AI',
    author_email='dev@lemon-ai.com',

    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=['pandas==2.0.3', 'clickhouse-driver[lz4]==0.2.6']
)