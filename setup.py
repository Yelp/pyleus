from distutils.command.bdist import bdist as _bdist
from distutils.core import Command
import os
import shutil
import subprocess
import sys

from setuptools import setup
from setuptools.command.sdist import sdist as _sdist

from pyleus import __version__
from pyleus import BASE_JAR

JAVA_SRC_DIR = "topology_builder/"
BASE_JAR_SRC = os.path.join(JAVA_SRC_DIR, "dist", BASE_JAR)
BASE_JAR_DST = os.path.join("pyleus", BASE_JAR)


class build_java(Command):

    description = "Build the topology base JAR"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def _make_jar(self):
        subprocess.check_call(["make", "-C", JAVA_SRC_DIR])

    def _copy_jar(self):
        shutil.copy(BASE_JAR_SRC, BASE_JAR_DST)

    def run(self):
        self._make_jar()
        self._copy_jar()


class bdist(_bdist):

    sub_commands = [('build_java', None)]

    def run(self):
        for cmd_name in self.get_sub_commands():
            self.run_command(cmd_name)

        _bdist.run(self)


class sdist(_sdist):

    sub_commands = [('build_java', None)]

    def run(self):
        for cmd_name in self.get_sub_commands():
            self.run_command(cmd_name)

        _sdist.run(self)


def readme():
    with open("README.rst") as f:
        return f.read()


extra_install_requires = []
if sys.version_info < (2, 7):
    # argparse is in the standard library of Python >= 2.7
    extra_install_requires.append("argparse")


setup(
    name="pyleus",
    version=__version__,
    author="Patrick Lucas",
    author_email="plucas@yelp.com",
    description="Standard library and deployment tools for using Python "
        "with Storm",
    long_description=readme(),
    url="http://pyleus.org",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Distributed Computing",
        "Development Status :: 4 - Beta",
    ],
    packages=[
        "pyleus", "pyleus.cli", "pyleus.cli.commands",
        "pyleus.storm", "pyleus.storm.serializers"],
    scripts=["scripts/pyleus"],
    install_requires=[
        "PyYAML",
        "msgpack-python",
        "virtualenv",
        "six",
    ] + extra_install_requires,
    package_data={'pyleus': [BASE_JAR]},
    cmdclass={
        'build_java': build_java,
        'bdist': bdist,
        'sdist': sdist,
    },
)
