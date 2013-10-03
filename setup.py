from setuptools import setup

from pyleus import __version__


setup(
    name='pyleus',
    version=__version__,
    author='Patrick Lucas',
    author_email='plucas@yelp.com',
    description='Standard library and deployment tools for using Python with Storm',
    packages=['pyleus'],
)
