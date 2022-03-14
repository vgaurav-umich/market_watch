from setuptools import find_packages, setup

setup(
    name='market_watch',
    packages=find_packages(exclude=('test*','testing*')),
    version='0.1.0',
    description='Capstone project for UMSI MADS program. RL bot for stock trading.',
    long_description='Capstone project for UMSI MADS program. RL bot for stock trading.',
    keywords="UMSI MADS",
    author='Matt, Egor, and Gaurav',
    license='MIT',
    install_requires=[
        'graphviz',
        'pygraphviz',
    ],
    setup_requires=[
        'pytest-runner',
        'jupyterlab',
        'ploomber',
        'pipenv'
    ],
    tests_require=['pytest', 'pytest-flask'],
)
