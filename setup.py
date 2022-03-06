from setuptools import find_packages, setup

setup(
    name='market_watch',
    packages=find_packages(exclude=('test*','testing*')),
    version='0.1.0',
    description='Capstone project for UMSI MADS program. RL bot for stock trading.',
    author='Matt, Egor, and Gaurav',
    license='MIT',
    install_requires=[
        'ploomber',
        'pipenv',
        'black',
        'graphviz',
        'pygraphviz',
    ],
    setup_requires=['pytest-runner', 'flake8', 'jupyterlab'],
    tests_require=['pytest', 'pytest-flask'],
)
