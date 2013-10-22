#!/usr/bin/python
"""Command-line tool for building a standalone, self-contained Pyleus topology
JAR ready to be submitted to a Storm cluster. If an optional requirements.txt
is provided, Pyleus will use virtualenv to collect and provide Python
dependencies to the running topology.

Args:
    TOPOLOGY_DIRECTORY - the directory where all the topology source files,
        the YAML file describing the topology (pyleus_topology.yaml) and the
        optional requirements.txt are found.

The script will attempt to ensure the contents of TOPOLOGY_DIRECTORY are in
order, that nothing will be improperly overwritten and that mandatory files are
present: pyleus_topology.yaml is always required and requirements.txt must
exist if --use-virtualenv is explicitly stated.

The output JAR is built from a common base JAR included in the pyleus package
by default, and will be named <TOPOLOGY_DIRECTORY>.jar.

Note: The names used for the YAML file and for the virtualenv CANNOT be changed
without modifying the Java code accordingly.
"""

import ConfigParser
import collections
import optparse
import glob
import re
import tempfile
import os
import shutil
import subprocess
import sys
import zipfile

CONFIG_SYSTEM_PATH = "/etc/pyleus.conf"
CONFIG_USER_PATH = "$HOME/.congif/pyleus.conf"
CONFIG_HOME_PATH = "$HOME/.pyleus.conf"

BASE_JAR_PATH = "minimal.jar"
RESOURCES_PATH = "resources/"
YAML_FILENAME = "pyleus_topology.yaml"
REQUIREMENTS_FILENAME = "requirements.txt"
VIRTUALENV = "pyleus_venv"

PROG = os.path.basename(sys.argv[0])
PYLEUS_ERROR_FMT = "{0}: error: {1}"

Configuration = collections.namedtuple(
    "Configuration",
    "config_file pypi_index_url base_jar output_jar "
    "use_virtualenv system pip_log verbose"
)

DEFAULTS = Configuration(
    config_file=None,
    pypi_index_url=None,
    base_jar=BASE_JAR_PATH,
    output_jar=None,
    use_virtualenv=None,
    system=False,
    pip_log=None,
    verbose=False,
)

class PyleusError(Exception):
    """Base class for pyleus specific exceptions"""
    def __str__(self):
        return "[{0}] {1}".format(type(self).__name__,
               ", ".join(str(i) for i in self.args))


class ConfigurationError(PyleusError): pass
class JarError(PyleusError): pass
class TopologyError(PyleusError): pass
class InvalidTopologyError(TopologyError): pass
class DependenciesError(TopologyError): pass


def _open_jar(base_jar):
    """Open the base jar file."""
    if not os.path.exists(base_jar):
        raise JarError("Base jar not found")

    if not zipfile.is_zipfile(base_jar):
        raise JarError("Base jar is not a jar file")

    zip_file = zipfile.ZipFile(base_jar, "r")

    return zip_file


def _validate_dir(topology_dir):
    """Ensure that the directory exists and is a directory"""
    if not os.path.exists(topology_dir):
        raise TopologyError("Topology directory not found: {0}".format(
            topology_dir))

    if not os.path.isdir(topology_dir):
        raise TopologyError("Topology directory is not a directory: {0}"
                            .format(topology_dir))


def _validate_yaml(yaml):
    """Ensure that TOPOLOGY_YAML exists inside the directory"""
    if not os.path.isfile(yaml):
        raise InvalidTopologyError("Topology YAML not found: {0}".format(yaml))


def _validate_req(req):
    """Ensure that requirements.txt exists inside the directory"""
    if not os.path.isfile(req):
        raise InvalidTopologyError("{0} file not found".format(
            REQUIREMENTS_FILENAME))


def _validate_venv(topology_dir, venv):
    """Ensure that VIRTUALENV does not exist inside the directory"""
    if os.path.exists(venv):
        raise InvalidTopologyError("Topology directory must not contain a "
                                   "file named {0}".format(venv))


def _validate_topology(topology_dir, yaml, req, venv, use_virtualenv):
    """Validate topology_dir to ensure that:

        - it exists and is a directory
        - TOPOLOGY_YAML exists inside
        - requirements.txt exists if --use-virtualenv was explicitly stated
        - nothing will be overwritten
    """
    _validate_dir(topology_dir)

    _validate_yaml(yaml)

    if use_virtualenv:
        _validate_req(req)
        _validate_venv(topology_dir, venv)


def _call_dep_cmd(cmd, cwd, stdout, stderr, err_msg):
    """Interface to any bash command related to dependencies management."""
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=stdout, stderr=stderr)
    out_data, _ = proc.communicate()
    if proc.returncode != 0:
        raise DependenciesError(err_msg)
    return out_data


def _is_pyleus_installed(tmp_dir, err_stream):
    """Check if pyleus is already installed in the virtualenv."""
    show_cmd = [os.path.join(VIRTUALENV, "bin", "pip"), "show", "pyleus"]
    out_data = _call_dep_cmd(
        show_cmd, cwd=tmp_dir,
        stdout=subprocess.PIPE, stderr=err_stream,
        err_msg="Failed to run pip show.")
    # pip show prints only if the package is already installed
    return out_data != ""


def _pip_install(tmp_dir, *args, **kwargs):
    """Interface to pip install comand."""
    pip_cmd = [os.path.join(VIRTUALENV, "bin", "pip"), "install"]

    for arg in args:
        pip_cmd.append(arg)

    if kwargs.get("req") is not None:
        pip_cmd += ["-r", kwargs["req"]]

    if kwargs.get("pypi_index_url") is not None:
        pip_cmd += ["-i", kwargs["pypi_index_url"]]

    if kwargs.get("pip_log") is not None:
        pip_cmd += ["--log", kwargs["pip_log"]]

    out_stream = None
    if kwargs.get("out_stream") is not None:
        out_stream = kwargs["out_stream"]

    err_msg = "Failed to install dependencies for this topology."
    " Run with --verbose for detailed info."
    if kwargs.get("err_msg") is not None:
        err_msg = kwargs["err_msg"]

    _call_dep_cmd(pip_cmd,
                  cwd=tmp_dir, stdout=out_stream, stderr=subprocess.STDOUT,
                  err_msg=err_msg)


def _virtualenv_pip_install(tmp_dir, req, **kwargs):
    """Create a virtualenv with the specified options and run `pip install -r
    requirements.txt`.

    Options:
        system-site-packages - creating the virtualenv with this flag,
        pip will not download and install in the virtualenv all the
        dependencies already installed system-wide.
        index-url - allow to specify the URL of the Python Package Index.
        pip-log - a verbose log generated by pip install
    """
    virtualenv_cmd = ["virtualenv", VIRTUALENV]

    if kwargs.get("system") is True:
        virtualenv_cmd.append("--system-site-packages")

    out_stream = None
    if kwargs.get("verbose") is False:
        out_stream = open(os.devnull, "w")

    _call_dep_cmd(virtualenv_cmd,
                  cwd=tmp_dir, stdout=out_stream, stderr=subprocess.STDOUT,
                  err_msg="Failed to install dependencies for this"
                  " topology. Failed to create virtualenv.")

    _pip_install(
        tmp_dir,
        req=req,
        pypi_index_url=kwargs.get("pypi_index_url"),
        pip_log=kwargs.get("pip_log"),
        out_stream=out_stream
    )

    # err_stream=out_stream
    # if verbose then errors to output, else errors to /dev/null
    if not _is_pyleus_installed(tmp_dir, err_stream=out_stream):
        _pip_install(
            tmp_dir,
            "pyleus",
            pypi_index_url=kwargs.get("pypi_index_url"),
            out_stream=out_stream,
            err_msg="Failed to install pyleus package."
            " Run with --verbose for detailed info."
        )


def _exclude_content(src, exclude_req):
    """Remove from the content list all paths matching the patterns
    in the exclude list.
    Filtering is applied only at the top level of the directory.
    """
    content = set(glob.glob(os.path.join(src, "*")))
    yaml = os.path.join(src, YAML_FILENAME)
    content -= set([yaml])
    if exclude_req:
        req = os.path.join(src, REQUIREMENTS_FILENAME)
        content -= set([req])
    return content


def _copy_dir_content(src, dst, exclude_req):
    """Copy the content of a directory excluding the paths
    matching the patterns in the exclude list.

    This functions is used instead of shutil.copytree() because
    the latter always creates a top level directory, while only
    the content need to be copied in this case.
    """
    content = _exclude_content(src, exclude_req)

    for t in content:
        if os.path.isdir(t):
            shutil.copytree(t, os.path.join(dst, os.path.basename(t)),
                            symlinks=True)
        else:
            shutil.copy2(t, dst)


def _zip_dir(src, arc):
    """Build a zip archive from the specified src.

    Note: If the archive already exists, files will be simply
    added to it, but the original archive will not be replaced.
    At the current state, this script enforce the creation of
    a brand new zip archive each time is run, otehrwise it will
    raise an exception.
    """
    src_re = re.compile(src + "/*")
    for root, dirs, files in os.walk(src):
        # hack for copying everithing but the top directory
        prefix = re.sub(src_re, "", root)
        for f in files:
            # zipfile creates directories if missing
            arc.write(os.path.join(root, f), os.path.join(prefix, f),
                      zipfile.ZIP_DEFLATED)


def _pack_jar(tmp_dir, output_jar):
    """Build a jar from the temporary directory."""
    if os.path.exists(output_jar):
        raise JarError("Output jar already exists: {0}".format(output_jar))

    zf = zipfile.ZipFile(output_jar, "w")
    try:
        _zip_dir(tmp_dir, zf)
    finally:
        zf.close()


def _is_virtualenv_required(configs, req):
    """Figure out if a virtuelenv is required, even implicitely"""
    # if use_virtualenv is undefined (None), it will be assigned on the base of
    # the requirements.txt file
    if configs.use_virtualenv is None:
        return os.path.isfile(req)
    return configs.use_virtualenv


def _inject(topology_dir, base_jar, output_jar, zip_file, tmp_dir, configs):
    """Coordinate the creation of the the topology JAR:

        - Validate the topology
        - Extract the base JAR into a temporary directory
        - Copy all source files into the directory
        - If using virtualenv, create it and install dependencies
        - Re-pack the temporary directory into the final JAR
    """
    yaml = os.path.join(topology_dir, YAML_FILENAME)
    req = os.path.join(topology_dir, REQUIREMENTS_FILENAME)
    venv = os.path.join(topology_dir, VIRTUALENV)

    use_virtualenv = _is_virtualenv_required(configs, req)

    _validate_topology(topology_dir, yaml, req, venv, use_virtualenv)

    # Extract pyleus base jar content in a tmp dir
    zip_file.extractall(tmp_dir)

    # Copy yaml into its directory
    shutil.copy2(yaml, os.path.join(tmp_dir, RESOURCES_PATH))

    # Add the topology directory skipping yaml and requirements
    _copy_dir_content(topology_dir, os.path.join(tmp_dir, RESOURCES_PATH),
                      exclude_req=use_virtualenv)

    # Virtualenv + pip install used to install dependencies listed in
    # requirements.txt
    if use_virtualenv:
        _virtualenv_pip_install(tmp_dir=os.path.join(tmp_dir, RESOURCES_PATH),
                                req=req,
                                system=configs.system,
                                pypi_index_url=configs.pypi_index_url,
                                pip_log=_expand_path(configs.pip_log),
                                verbose=configs.verbose)

    # Pack the tmp directory into a jar
    _pack_jar(tmp_dir, output_jar)


def _expand_path(path):
    """Return the corresponding absolute path after variables expansion."""
    if path is None:
        return None
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))


def _build_output_path(output_arg, topology_dir):
    """Return the absolute path of the output jar file.

    Default basename:
        TOPOLOGY_DIRECTORY.jar
    """
    if output_arg is not None:
        return _expand_path(output_arg)
    else:
        return _expand_path(os.path.basename(topology_dir) + ".jar")


def _update_configuration(config, update_dict):
    """Update configuration with new values passed as dictionary"""
    tmp = config._asdict()
    tmp.update(update_dict)
    return Configuration(**tmp)


def _validate_config_file(config_file):
    """Ensure that config_file exists and is a file"""
    if not os.path.exists(config_file):
        raise ConfigurationError("Specified configuration file not"
                                 "found: {0}".format(config_file))
    if not os.path.isfile(config_file):
        raise ConfigurationError("Specified configuration file is not"
                                 " a file: {0}".format(config_file))


def _load_configuration(cmd_line_file):
    """Load configurations from the more generic to the
    more specific configuration file. The latter configurations
    override the previous one.
    If a file is specified from command line, it  is considered
    the most specific.

    Returns:
    Configuration named tuple
    """
    config_files_hierarchy = [
        _expand_path(CONFIG_SYSTEM_PATH),
        _expand_path(CONFIG_USER_PATH),
        _expand_path(CONFIG_HOME_PATH)
    ]

    if cmd_line_file is not None:
        _validate_config_file(cmd_line_file)
        config_files_hierarchy.append(cmd_line_file)

    config = ConfigParser.SafeConfigParser()
    config.read(config_files_hierarchy)

    configs = _update_configuration(
        DEFAULTS,
        dict(
            (config_name, config_value)
            for section in config.sections()
            for config_name, config_value in config.items(section)
        )
    )
    return configs


def main():
    """Parse command-line arguments and invoke _inject()"""
    parser = optparse.OptionParser(
        usage="usage: %prog [options] TOPOLOGY_DIRECTORY",
        description="Build up a Storm jar from a topology source directory")

    parser.add_option("-o", "--out", dest="output_jar", default=None,
                      help="Path of the jar file that will contain"
                      " all the dependencies and the resources")
    parser.add_option("--use-virtualenv", dest="use_virtualenv",
                      default=None, action="store_true",
                      help="Use virtualenv and pip install for dependencies."
                      " Your TOPOLOGY_DIRECTORY must contain a file named {0}"
                      .format(REQUIREMENTS_FILENAME))
    parser.add_option("--no-use-virtualenv",
                      dest="use_virtualenv", action="store_false",
                      help="Do not use virtualenv and pip for dependencies")
    parser.add_option("-s", "--system-site-packages", dest="system",
                      default=False, action="store_true",
                      help="Do not install packages already present"
                      "on your system")
    parser.add_option("--log", dest="pip_log", default=None,
                      help="Log location for pip")
    parser.add_option("-v", "--verbose", dest="verbose",
                      default=False, action="store_true",
                      help="Verbose")
    parser.add_option("-c", "--config", dest="config_file", default=None,
                      help="Pyleus configuration file")
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments")

    # Load configurations into a Configuration named tuple
    try:
        configs = _load_configuration(_expand_path(options.config_file))
    except PyleusError as e:
        sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))

    # Update configuration with command line values
    configs = _update_configuration(configs, vars(options))

    topology_dir = _expand_path(args[0])
    base_jar = _expand_path(configs.base_jar)
    output_jar = _build_output_path(configs.output_jar, topology_dir)

    # Check for output path existence for early failure
    if os.path.exists(output_jar):
        e = JarError("Output jar already exist: {0}".format(output_jar))
        sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))

    try:
        # Open the base jar as a zip
        zip_file = _open_jar(base_jar)
    except PyleusError as e:
        sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))

    try:
        # Everything will be copied in a tmp directory
        tmp_dir = tempfile.mkdtemp()
        try:
            _inject(topology_dir, base_jar, output_jar,
                    zip_file, tmp_dir, configs)
        except PyleusError as e:
            sys.exit(PYLEUS_ERROR_FMT.format(PROG, str(e)))
        finally:
            shutil.rmtree(tmp_dir)
    finally:
        zip_file.close()


if __name__ == "__main__":
    main()
