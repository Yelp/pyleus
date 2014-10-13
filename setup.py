from distutils.command.bdist import bdist as _bdist
from distutils.core import Command
import os
import subprocess
import sys

from setuptools import setup
from setuptools.command.sdist import sdist as _sdist

from pyleus import __version__
from pyleus import BASE_JAR, BASE_JAR_INSTALL_DIR

JAVA_SRC_DIR = "topology_builder/"
BASE_JAR_SRC = os.path.join(JAVA_SRC_DIR, "dist", BASE_JAR)


class build_java(Command):

    description = "Build the topology base JAR"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def _make_jar(self):
        subprocess.check_call(["make", "-C", JAVA_SRC_DIR])

    def run(self):
        self._make_jar()


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
    packages=[
        "pyleus", "pyleus.cli", "pyleus.cli.commands",
        "pyleus.storm", "pyleus.storm.serializers"],
    scripts=["scripts/pyleus"],
    install_requires=[
        "PyYAML",
        "msgpack-python",
    ] + extra_install_requires,
    data_files=[(BASE_JAR_INSTALL_DIR, [BASE_JAR_SRC])],
    cmdclass={
        'build_java': build_java,
        'bdist': bdist,
        'sdist': sdist,
    },
)
