"""Logic for building a jar from a pyleus topology directory.

Other modules should only call build_topology_jar passing it the configurations
object. The caller function should handle PyleusError exceptions.
"""
from __future__ import absolute_import

import glob
import re
import tempfile
import os
import shutil
import zipfile

from pyleus.cli.virtualenv_proxy import VirtualenvProxy
from pyleus.utils import expand_path
from pyleus.exception import InvalidTopologyError
from pyleus.exception import JarError
from pyleus.exception import TopologyError


RESOURCES_PATH = "resources"
YAML_FILENAME = "pyleus_topology.yaml"
REQUIREMENTS_FILENAME = "requirements.txt"
VIRTUALENV_NAME = "pyleus_venv"


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

    # topology_dir should not include a file named as pyleus default virtualenv
    _validate_venv(topology_dir, venv)

    if use_virtualenv:
        _validate_req(req)


def _set_up_virtualenv(venv_name, tmp_dir, req,
                       include_packages, system_site_packages,
                       pypi_index_url, verbose):
    """Create a virtualenv with the specified options and the default packages
    specified in configuration. Then run `pip install -r requirements.txt`.
    """
    venv = VirtualenvProxy(
        venv_name, tmp_dir,
        system_site_packages,
        pypi_index_url,
        verbose
    )

    packages = ["pyleus"]
    if include_packages is not None:
        packages += include_packages
    for package in packages:
        if not venv.is_package_installed(package):
            venv.install_package(package)

    venv.install_from_requirements(req)


def _is_virtualenv_required(req):
    """Figure out if a virtuelenv is implicitely required"""
    return os.path.isfile(req)


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
    """Copy the content of a directory excluding the yaml file
    and requirements.txt exclude_req is True.

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


def _create_pyleus_jar(topology_dir, base_jar, output_jar, zip_file, tmp_dir,
                       use_virtualenv, include_packages, system_site_packages,
                       pypi_index_url, verbose):
    """Coordinate the creation of the the topology JAR:

        - Validate the topology
        - Extract the base JAR into a temporary directory
        - Copy all source files into the directory
        - If using virtualenv, create it and install dependencies
        - Re-pack the temporary directory into the final JAR
    """
    yaml = os.path.join(topology_dir, YAML_FILENAME)
    req = os.path.join(topology_dir, REQUIREMENTS_FILENAME)
    venv = os.path.join(topology_dir, VIRTUALENV_NAME)

    if use_virtualenv is None:
        use_virtualenv = _is_virtualenv_required(req)

    _validate_topology(topology_dir, yaml, req, venv, use_virtualenv)

    # Extract pyleus base jar content in a tmp dir
    zip_file.extractall(tmp_dir)

    # Copy yaml into its directory
    resources_dir = os.path.join(tmp_dir, RESOURCES_PATH)
    os.mkdir(resources_dir)
    shutil.copy2(yaml, resources_dir)

    # Add the topology directory skipping yaml and requirements
    _copy_dir_content(topology_dir, os.path.join(tmp_dir, RESOURCES_PATH),
                      exclude_req=use_virtualenv)

    # Virtualenv + pip install used to install dependencies listed in
    # requirements.txt
    if use_virtualenv:
        _set_up_virtualenv(venv_name=VIRTUALENV_NAME,
                           tmp_dir=os.path.join(tmp_dir, RESOURCES_PATH),
                           req=req,
                           include_packages=include_packages,
                           system_site_packages=system_site_packages,
                           pypi_index_url=pypi_index_url,
                           verbose=verbose)

    # Pack the tmp directory into a jar
    _pack_jar(tmp_dir, output_jar)


def _build_output_path(output_arg, topology_dir):
    """Return the absolute path of the output jar file.

    Default basename:
        TOPOLOGY_DIRECTORY.jar
    """
    if output_arg is not None:
        return expand_path(output_arg)
    else:
        return expand_path(os.path.basename(topology_dir) + ".jar")


def build_topology_jar(configs):
    """Parse command-line arguments and invoke _create_pyleus_jar()"""
    # Expand paths if necessary
    topology_dir = expand_path(configs.topology_dir)
    base_jar = expand_path(configs.base_jar)
    output_jar = _build_output_path(configs.output_jar, topology_dir)

    # Extract list of packages to always include from configuration
    # NOTE: right now if package==version is specified in configuration,
    # the package will be installed normally, but will fail the
    # is_installed check
    include_packages = None
    if configs.include_packages is not None:
        include_packages = configs.include_packages.split(" ")

    # Check for output path existence for early failure
    if os.path.exists(output_jar):
        raise JarError("Output jar already exist: {0}".format(output_jar))

    # Open the base jar as a zip
    zip_file = _open_jar(base_jar)

    try:
        # Everything will be copied in a tmp directory
        tmp_dir = tempfile.mkdtemp()
        try:
            _create_pyleus_jar(
                topology_dir=topology_dir,
                base_jar=base_jar,
                output_jar=output_jar,
                zip_file=zip_file,
                tmp_dir=tmp_dir,
                use_virtualenv=configs.use_virtualenv,
                include_packages=include_packages,
                system_site_packages=configs.system_site_packages,
                pypi_index_url=configs.pypi_index_url,
                verbose=configs.verbose,
            )
        finally:
            shutil.rmtree(tmp_dir)
    finally:
        zip_file.close()

    return output_jar
