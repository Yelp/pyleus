"""Logic for building a jar from a pyleus topology directory.

Other modules should only call build_topology_jar passing it the configurations
object. The caller function should handle PyleusError exceptions.
"""
from __future__ import absolute_import

import glob
import logging
import os
import re
import shutil
import tempfile
import yaml
import zipfile

from pyleus import __version__
from pyleus.cli.topology_spec import TopologySpec
from pyleus.cli.virtualenv_proxy import VirtualenvProxy
from pyleus.compat import StringIO
from pyleus.storm.component import DESCRIBE_OPT
from pyleus.exception import InvalidTopologyError
from pyleus.exception import JarError
from pyleus.utils import expand_path

RESOURCES_PATH = "resources"
YAML_FILENAME = "pyleus_topology.yaml"
DEFAULT_REQUIREMENTS_FILENAME = "requirements.txt"
VIRTUALENV_NAME = "pyleus_venv"

log = logging.getLogger(__name__)


def _open_jar(base_jar):
    """Open the base jar file."""
    if not os.path.exists(base_jar):
        raise JarError("Base jar not found")

    if not zipfile.is_zipfile(base_jar):
        raise JarError("Base jar is not a jar file")

    zip_file = zipfile.ZipFile(base_jar, "r")

    return zip_file


def _zip_dir(src, arc):
    """Build a zip archive from the specified src.

    Note: If the archive already exists, files will be simply
    added to it, but the original archive will not be replaced.
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
    zf = zipfile.ZipFile(output_jar, "w")
    try:
        _zip_dir(tmp_dir, zf)
    finally:
        zf.close()


def _validate_venv(topology_dir, venv):
    """Ensure that VIRTUALENV does not exist inside the directory"""
    if os.path.exists(venv):
        raise InvalidTopologyError("Topology directory must not contain a "
                                   "file named {0}".format(venv))


def _path_contained_by(containing_path, path):
    """Return whether path is a subpath of containing_path"""
    # Call to os.path.join adds a trailing separator to real_containing_path
    # without which os.path.commonprefix can be unreliable.
    real_containing_path = os.path.join(os.path.realpath(containing_path), '')
    real_path = os.path.realpath(path)
    common_prefix = os.path.commonprefix([real_containing_path, real_path])
    return common_prefix == real_containing_path


def _remove_pyleus_base_jar(venv):
    """Remove the Pyleus base jar from the virtualenv since it's redundant and
    takes up space. See PYLEUS-74.

    This function verifies that base_jar_path is actually inside the virtualenv
    before removing it. If the user is using --system-site-packages and has
    pyleus installed on their system, base_jar_path is actually outside the
    virtualenv, and we don't want to attempt its removal in that case.
    """
    base_jar_path = venv.execute_module("pyleus._base_jar",
                                        cwd=venv.path).strip()
    if _path_contained_by(venv.path, base_jar_path):
        os.remove(base_jar_path)


def _set_up_virtualenv(venv_name, tmp_dir, req,
                       include_packages, system_site_packages,
                       pypi_index_url, python_interpreter, verbose):
    """Create a virtualenv with the specified options and the default packages
    specified in configuration. Then run `pip install -r [requirements file]`.
    """
    venv = VirtualenvProxy(
        os.path.join(tmp_dir, venv_name),
        system_site_packages=system_site_packages,
        pypi_index_url=pypi_index_url,
        python_interpreter=python_interpreter,
        verbose=verbose
    )

    packages = ["pyleus=={0}".format(__version__)]
    if include_packages is not None:
        packages += include_packages

    for package in packages:
        venv.install_package(package)

    if req is not None:
        venv.install_from_requirements(req)

    _remove_pyleus_base_jar(venv)

    return venv


def _assemble_full_topology_yaml(spec, venv, resources_dir):
    """Assemble a full version of the topology yaml file given by the user
    adding to it the information coming from the python source files.
    """
    for component in spec.topology:
        if component.type == "python":
            log.debug('Assemble component module: {0}'.format(component.module))
            description = venv.execute_module(module=component.module,
                                              args=[DESCRIBE_OPT],
                                              cwd=resources_dir)
            module_spec = yaml.load(description)
            component.update_from_module(module_spec)

    spec.verify_groupings()

    new_yaml = StringIO()
    yaml.dump(spec.asdict(), new_yaml)
    return new_yaml.getvalue()


def _content_to_copy(src, exclude):
    """Return a set of top-level content to copy, excluding exact matches
    from the exclude list.
    """
    content = set(glob.glob(os.path.join(src, "*")))
    content -= set(exclude)
    return content


def _copy_dir_content(src, dst, exclude):
    """Copy the content of a directory excluding the yaml file
    and requirements file.

    This functions is used instead of shutil.copytree() because
    the latter always creates a top level directory, while only
    the content need to be copied in this case.
    """
    content = _content_to_copy(src, exclude)

    for t in content:
        if os.path.isdir(t):
            shutil.copytree(t, os.path.join(dst, os.path.basename(t)),
                            symlinks=True)
        else:
            shutil.copy2(t, dst)


def _create_pyleus_jar(original_topology_spec, topology_dir, base_jar,
                       output_jar, zip_file, tmp_dir, include_packages,
                       system_site_packages, pypi_index_url, verbose):
    """Coordinate the creation of the the topology JAR:

        - Validate the topology
        - Extract the base JAR into a temporary directory
        - Copy all source files into the directory
        - If using virtualenv, create it and install dependencies
        - Re-pack the temporary directory into the final JAR
    """
    requirements_filename = original_topology_spec.requirements_filename
    if not requirements_filename:
        requirements_filename = DEFAULT_REQUIREMENTS_FILENAME

    python_interpreter = original_topology_spec.python_interpreter

    venv = os.path.join(topology_dir, VIRTUALENV_NAME)
    req = os.path.join(topology_dir, requirements_filename)
    if not os.path.isfile(req):
        req = None

    _validate_venv(topology_dir, venv)

    # Extract pyleus base jar content in a tmp dir
    zip_file.extractall(tmp_dir)

    # Create resources directory
    resources_dir = os.path.join(tmp_dir, RESOURCES_PATH)
    os.mkdir(resources_dir)

    # Add the topology directory skipping yaml and requirements
    _copy_dir_content(
        src=topology_dir,
        dst=resources_dir,
        exclude=[venv, req, output_jar],
    )

    venv = _set_up_virtualenv(
        venv_name=VIRTUALENV_NAME,
        tmp_dir=resources_dir,
        req=req,
        include_packages=include_packages,
        system_site_packages=system_site_packages,
        pypi_index_url=pypi_index_url,
        python_interpreter=python_interpreter,
        verbose=verbose)

    # Assemble the full version of the topolgy yaml file from the user yaml and
    # the python code
    new_yaml = _assemble_full_topology_yaml(
        spec=original_topology_spec,
        venv=venv,
        resources_dir=resources_dir)

    # Copy the new yaml file into its directory, overwriting the old one
    jar_yaml = os.path.join(resources_dir, YAML_FILENAME)
    with open(jar_yaml, 'w') as f:
        f.write(new_yaml)

    # Pack the tmp directory into a jar
    _pack_jar(tmp_dir, output_jar)


def _build_output_path(output_arg, topology_name):
    """Return the absolute path of the output jar file.

    Default basename:
        TOPOLOGY_DIRECTORY.jar
    """
    if output_arg is not None:
        return expand_path(output_arg)
    else:
        return expand_path(topology_name + ".jar")


def parse_original_topology(topology_path):
    with open(topology_path) as f:
        yaml_spec = yaml.load(f)

    return TopologySpec(yaml_spec)


def build_topology_jar(configs):
    """Parse command-line arguments and invoke _create_pyleus_jar()"""
    # Expand paths if necessary
    topology_path = expand_path(configs.topology_path)
    topology_dir = expand_path(os.path.dirname(topology_path))
    base_jar = expand_path(configs.base_jar)

    # Parse the topology
    original_topology_spec = parse_original_topology(topology_path)

    output_jar = _build_output_path(configs.output_jar,
                                    original_topology_spec.name)

    # Extract list of packages to always include from configuration
    include_packages = None
    if configs.include_packages is not None:
        include_packages = configs.include_packages.split(" ")

    # Open the base jar as a zip
    zip_file = _open_jar(base_jar)

    try:
        # Everything will be copied in a tmp directory
        tmp_dir = tempfile.mkdtemp()
        try:
            _create_pyleus_jar(
                original_topology_spec=original_topology_spec,
                topology_dir=topology_dir,
                base_jar=base_jar,
                output_jar=output_jar,
                zip_file=zip_file,
                tmp_dir=tmp_dir,
                include_packages=include_packages,
                system_site_packages=configs.system_site_packages,
                pypi_index_url=configs.pypi_index_url,
                verbose=configs.verbose,
            )
        finally:
            shutil.rmtree(tmp_dir)
    finally:
        zip_file.close()
