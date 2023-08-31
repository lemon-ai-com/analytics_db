from setuptools import setup

from analytics_db import __version__

setup(
    name='analytics_db',
    version=__version__,

    url='https://github.com/lemon-ai-com/analytics_db',
    author='Lemon AI',
    author_email='dev@lemon-ai.com',

    py_modules=['analytics_db'],
    install_requires=['pandas==2.0.3', 'clickhouse-driver[lz4]==0.2.6']
)