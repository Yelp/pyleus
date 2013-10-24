"""Pyleus configuration module
"""

import collections
import ConfigParser
import os

from pyleus.utils import expand_path
from pyleus.exception import ConfigurationError

# Configuration files paths in order of increasing precedence
CONFIG_FILES_PATH = [
    "/etc/pyleus.conf",
    "~/.config/pyleus.conf",
    "~/.pyleus.conf"
]

BASE_JAR_PATH = "minimal.jar"

Configuration = collections.namedtuple(
    "Configuration",
    "base_jar config_file func include_packages output_jar pip_log \
     pypi_index_url system topology_dir use_virtualenv verbose"
)


DEFAULTS = Configuration(
    base_jar=BASE_JAR_PATH,
    config_file=None,
    func=None,
    include_packages=None,
    output_jar=None,
    pip_log=None,
    pypi_index_url=None,
    system=False,
    topology_dir=None,
    use_virtualenv=None,
    verbose=False,
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
    """Update configuration with new values passed as dictionary"""
    tmp = config._asdict()
    tmp.update(update_dict)
    return Configuration(**tmp)


def load_configuration(cmd_line_file):
    """Load configurations from the more generic to the
    more specific configuration file. The latter configurations
    override the previous one.
    If a file is specified from command line, it  is considered
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
