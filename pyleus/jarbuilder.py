#!/usr/bin/python

import optparse
import glob
import re
import tempfile
import os
import shutil
import subprocess
import sys
import zipfile


BASE_JAR_PATH = "minimal.jar"
RESOURCES_PATH = "resources/"
YAML_DIRECTORY = "topology_yaml"
REQUIREMENTS_FILE = "requirements.txt"
TEST_DIRECTORY = "test"
VIRTUALENV = "venv"
PACKAGE_INDEX_URL = "http://pypi-dev.yelpcorp.com/simple/"
DEPENDENCIES_PATH = "venv/lib/python2.6/site-packages/"
PIP_FILES_EXTENSIONS = ["*.egg", "*.egg-info", "*.pth"]

PROG = os.path.basename(sys.argv[0])
PYLEUS_ERROR_FMT = "{0}: error: {1}"


class PyleusError(Exception):
    def __str__(self):
        return "[{0}] {1}".format(type(self).__name__, ", ".join(str(i) for i in self.args))


class JarError(PyleusError): pass


class TopologyError(PyleusError): pass


class InvalidTopologyError(TopologyError): pass


class DependenciesError(TopologyError): pass


def _open_jar(base_jar):
    if not os.path.exists(base_jar):
        raise JarError("Base jar not found")

    if not zipfile.is_zipfile(base_jar):
        raise JarError("Base jar is not a jar file")

    zip_file = zipfile.ZipFile(base_jar, "r")

    return zip_file


def _extract(zip_file, tmp_dir):
    zip_file.extractall(tmp_dir)


def _validate_topology(topology_dir):
    if not os.path.exists(topology_dir):
        raise TopologyError("Topology directory not found")

    if not os.path.isdir(topology_dir):
        raise TopologyError("Topology directory is not a directory")

    yaml = glob.glob(os.path.join(topology_dir, "*.yaml"))
    if len(yaml) == 0:
        raise InvalidTopologyError("Yaml file not found")
    elif len(yaml) > 1:
        raise InvalidTopologyError("More than one yaml file."
                                   " Topology shoud be specified in single yaml file")

    req = glob.glob(os.path.join(topology_dir, REQUIREMENTS_FILE))
    if len(req) == 0:
        raise InvalidTopologyError("{0} file not found".format(REQUIREMENTS_FILE))
    elif len(req) > 1:
        raise InvalidTopologyError("More than one {0} file."
                                   " Requirements shoud be specified in single file"
                                   .format(REQUIREMENTS_FILE))

    return yaml[0], req[0]


def _pack_jar(tmp_dir, output_jar):
    if os.path.exists(output_jar):
        raise JarError("Output jar already exist")

    zf = zipfile.ZipFile(output_jar, "w")
    try:
        _zip_dir(tmp_dir, zf)
    finally:
        zf.close()


def _virtualenv_pip_install(tmp_dir, req, options):
    virtualenv_cmd = ["virtualenv", VIRTUALENV]
    if options.system:
        virtualenv_cmd.append("--system-site-packages")

    pip_cmd = ["{0}/bin/pip".format(VIRTUALENV), "install",
               "-i", PACKAGE_INDEX_URL,
               "-r", req]
    if options.pip_log:
        pip_cmd += ["--log", options.pip_log]

    out_stream = None
    if options.quiet:
        out_stream = open(os.devnull, "w")

    ret_code = subprocess.call(virtualenv_cmd, cwd=tmp_dir, stdout=out_stream, stderr=subprocess.STDOUT)
    if ret_code != 0:
        raise DependenciesError("Failed to install dependencies for this topology.\n"
                                "Failed to create virtualenv.")

    ret_code = subprocess.call(pip_cmd, cwd=tmp_dir, stdout=out_stream, stderr=subprocess.STDOUT)
    if ret_code != 0:
        raise DependenciesError("Failed to install dependencies for this topology.\n"
                                "Run with --log for detailed info.")


def _copy_yaml(yaml, dst):
    os.mkdir(os.path.join(dst, YAML_DIRECTORY))
    shutil.copy2(yaml, os.path.join(dst, YAML_DIRECTORY))


def _copy_dir_content(src, dst, excluded):
    # From all content
    content = set(glob.glob(os.path.join(src, "*")))
    # Exclude everything matching the patterns specified in excluded
    content -= set([q for x in excluded for q in glob.glob(os.path.join(src, x))])
    # Then exclude links
    content = [t for t in content if not os.path.islink(t)]

    for t in content:
        if os.path.isdir(t):
            shutil.copytree(t, os.path.join(dst, os.path.basename(t)))
        else:
            shutil.copy2(os.path.join(src, t), dst)


def _zip_dir(src, arc):
    src_re = re.compile(src + "/*")
    for root, dirs, files in os.walk(src):
        # hack for copying everithing but the top directory
        prefix = re.sub(src_re, "", root)
        for f in files:
            # zipfile creates directories if missing
            arc.write(os.path.join(root, f), os.path.join(prefix, f), zipfile.ZIP_DEFLATED)


def _inject(topology_dir, base_jar, output_jar, zip_file, tmp_dir, options):

    # Extract pyleus base jar content in a tmp dir
    _extract(zip_file, tmp_dir)

    # Validate topolgy and return requirements and yaml file path
    yaml, req = _validate_topology(topology_dir)

    # Copy yaml into its directory
    _copy_yaml(yaml, os.path.join(tmp_dir, RESOURCES_PATH))

    # Add the topology directory skipping yaml, requirements and the test directory
    topo_exclude = [yaml, req, TEST_DIRECTORY]
    _copy_dir_content(topology_dir, os.path.join(tmp_dir, RESOURCES_PATH), topo_exclude)

    # Virtualenv + pip install used to install dependencies listed in
    # requirements.txt
    _virtualenv_pip_install(os.path.join(tmp_dir, RESOURCES_PATH), req, options)

    # Pack the tmp directory into a jar
    _pack_jar(tmp_dir, output_jar)


def _build_output_path(output_arg, topology_dir):
    if output_arg is not None:
        output_jar = os.path.abspath(output_arg)
    else:
        output_jar = os.path.abspath(os.path.basename(topology_dir) + ".jar")
    return output_jar


def main():
    parser = optparse.OptionParser(
                usage="usage: {0} [options] <topology_directory>".format(PROG),
                description="Build up a storm jar from a topology source directory")
    parser.add_option("-b", "--base",
                      dest="base_jar",
                      metavar="<base_jar>",
                      default=BASE_JAR_PATH,
                      help="pyleus base jar file path")
    parser.add_option("-o", "--out",
                      dest="output_jar",
                      metavar="<output_jar>",
                      help="path of the jar file that will contain all the dependencies and the resources")
    parser.add_option("-s", "--system-packages",
                      dest="system",
                      action="store_true",
                      help="do not install packages already present in your system")
    parser.add_option("--log",
                      dest="pip_log",
                      metavar="<file>",
                      help="log location for pip")
    parser.add_option("-q", "--quiet",
                      dest="quiet",
                      action="store_true",
                      help="quiet")
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments")

    # Transform each path in its absolute version
    topology_dir = os.path.abspath(args[0])
    base_jar = os.path.abspath(options.base_jar)
    output_jar = _build_output_path(options.output_jar, topology_dir)
    if options.pip_log is not None:
        options.pip_log = os.path.abspath(options.pip_log)

    try:
        # Open the base jar as a zip
        zip_file = _open_jar(base_jar)
    except PyleusError as e:
        sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))

    try:
        # Everything will be copied in a tmp directory
        tmp_dir = tempfile.mkdtemp()
        try:
            _inject(topology_dir, base_jar, output_jar, zip_file, tmp_dir, options)
        except PyleusError as e:
            sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))
        finally:
            shutil.rmtree(tmp_dir)
    finally:
        zip_file.close()


if __name__ == "__main__":
    main()
