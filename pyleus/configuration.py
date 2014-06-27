"""Pyleus configuration module
"""
from __future__ import absolute_import

import collections
import ConfigParser
import os

from pyleus import BASE_JAR_PATH
from pyleus.utils import expand_path
from pyleus.exception import ConfigurationError


# Configuration files paths in order of increasing precedence
CONFIG_FILES_PATH = [
    "/etc/pyleus.conf",
    "~/.config/pyleus.conf",
    "~/.pyleus.conf"
]

Configuration = collections.namedtuple(
    "Configuration",
    "base_jar config_file debug func include_packages output_jar \
     pypi_index_url storm_cluster_ip storm_cmd_path system_site_packages \
     topology_path topology_jar topology_name verbose wait_time jvm_opts"
)


DEFAULTS = Configuration(
    base_jar=BASE_JAR_PATH,
    config_file=None,
    debug=False,
    func=None,
    include_packages=None,
    output_jar=None,
    pypi_index_url=None,
    storm_cluster_ip=None,
    storm_cmd_path=None,
    system_site_packages=False,
    topology_path="pyleus_topology.yaml",
    topology_jar=None,
    topology_name=None,
    verbose=False,
    wait_time=None,
    jvm_opts=None,
)


def _validate_config_file(config_file):
    """Ensure that config_file exists and is a file"""
    if not os.path.exists(config_file):
        raise ConfigurationError("Specified configuration file not"
                                 " found: {0}".format(config_file))
    if not os.path.isfile(config_file):
        raise ConfigurationError("Specified configuration file is not"
                                 " a file: {0}".format(config_file))


def update_configuration(config, update_dict):
    """Update configuration with new values passed as dictionary.
    returns: new configuration namedtuple"""
    tmp = config._asdict()
    tmp.update(update_dict)
    return Configuration(**tmp)


def load_configuration(cmd_line_file):
    """Load configurations from the more generic to the
    more specific configuration file. The latter configurations
    override the previous one.
    If a file is specified from command line, it is considered
    the most specific.

    Returns:
    Configuration named tuple
    """
    config_files_hierarchy = [expand_path(c) for c in CONFIG_FILES_PATH]

    if cmd_line_file is not None:
        _validate_config_file(cmd_line_file)
        config_files_hierarchy.append(cmd_line_file)

    config = ConfigParser.SafeConfigParser()
    config.read(config_files_hierarchy)

    configs = update_configuration(
        DEFAULTS,
        dict(
            (config_name, config_value)
            for section in config.sections()
            for config_name, config_value in config.items(section)
        )
    )
    return configs
