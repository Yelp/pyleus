from distutils.command.bdist import bdist as _bdist
from distutils.core import Command
import os
import shutil
import subprocess
import sys

from setuptools import setup
from setuptools.command.sdist import sdist as _sdist

from pyleus import __version__
from pyleus import BASE_JAR, BASE_JAR_INSTALL_DIR

JAVA_SRC_DIR = "../yaml_topo"
BASE_JAR_SRC = os.path.join(JAVA_SRC_DIR, "dist", BASE_JAR)
BASE_JAR_DST = os.path.join("java", BASE_JAR)


install_requires = []
if sys.version_info < (2, 7):
    install_requires.append("argparse==1.2.1")


class build_java(Command):

    description = "Build the topology base JAR and add it to the manifest."
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def _make_jar(self):
        subprocess.check_call(["make", "-C", JAVA_SRC_DIR])

    def _move_jar(self):
        dest_dir = os.path.dirname(BASE_JAR_DST)
        if not os.path.isdir(dest_dir):
            os.mkdir(dest_dir)
        shutil.copy2(BASE_JAR_SRC, BASE_JAR_DST)

    def _write_manifest(self):
        with open("MANIFEST.in", 'w') as f:
            f.write("include %s\n" % BASE_JAR_DST)

    def run(self):
        self._make_jar()
        self._move_jar()

        if sys.version_info < (2, 7):
            self._write_manifest()


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


setup(
    name="pyleus",
    version=__version__,
    author="Patrick Lucas",
    author_email="plucas@yelp.com",
    description="Standard library and deployment tools for using Python "
        "with Storm",
    packages=["pyleus", "pyleus.cli"],
    scripts=["scripts/pyleus"],
    install_requires=install_requires,
    data_files=[(BASE_JAR_INSTALL_DIR, [BASE_JAR_DST])],
    cmdclass={
        'build_java': build_java,
        'bdist': bdist,
        'sdist': sdist,
    },
)
