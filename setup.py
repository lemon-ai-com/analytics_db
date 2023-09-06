from setuptools import setup


setup(
    name='analytics_db',
    version='0.1.0',

    url='https://github.com/lemon-ai-com/analytics_db',
    author='Lemon AI',
    author_email='dev@lemon-ai.com',

    py_modules=['analytics_db'],
    install_requires=['pandas==2.0.3', 'clickhouse-driver[lz4]==0.2.6']
)