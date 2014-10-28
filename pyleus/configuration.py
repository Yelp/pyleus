"""Configuration defaults and loading functions.

Pyleus will look for configuration files in the following file paths in order
of increasing precedence. The latter configuration overrides the previous one.

#. /etc/pyleus.conf
#. ~/.config/pyleus.conf
#. ~/.pyleus.conf

You can always specify a configuration file when running any pyleus CLI command
as following:

``$ pyleus -c /path/to/config_file CMD``

This will override previous configurations.

Configuration file example
--------------------------
The following file contains all options you can configure for all pyleus
invocations.

.. code-block:: ini

   [storm]
   # path to Storm executable (pyleus will automatically look in PATH)
   storm_cmd_path: /usr/share/storm/bin/storm

   # optional: use -n option of pyleus CLI instead
   nimbus_host: 10.11.12.13

   # optional: use -p option of pyleus CLI instead
   nimbus_port: 6628

   # java options to pass to Storm CLI
   jvm_opts: -Djava.io.tmpdir=/home/myuser/tmp

   [build]
   # PyPI server to use during the build of your topologies
   pypi_index_url: http://pypi.ninjacorp.com/simple/

   # always use system-site-packages for pyleus virtualenvs (default: false)
   system_site_packages: true

   # list of packages to always include in your topologies
   include_packages: foo bar<4.0 baz==0.1
"""
from __future__ import absolute_import

import collections
import os

from pyleus import BASE_JAR_PATH
from pyleus.utils import expand_path
from pyleus.exception import ConfigurationError
from pyleus.compat import configparser


# Configuration files paths in order of increasing precedence
# Please keep in sync with module docstring
CONFIG_FILES_PATH = [
    "/etc/pyleus.conf",
    "~/.config/pyleus.conf",
    "~/.pyleus.conf"
]

Configuration = collections.namedtuple(
    "Configuration",
    "base_jar config_file debug func include_packages output_jar \
     pypi_index_url nimbus_host nimbus_port storm_cmd_path \
     system_site_packages topology_path topology_jar topology_name verbose \
     wait_time jvm_opts"
)
"""Namedtuple containing all pyleus configuration values."""


DEFAULTS = Configuration(
    base_jar=BASE_JAR_PATH,
    config_file=None,
    debug=False,
    func=None,
    include_packages=None,
    output_jar=None,
    pypi_index_url=None,
    nimbus_host=None,
    nimbus_port=None,
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
    """Ensure that config_file exists and is a file."""
    if not os.path.exists(config_file):
        raise ConfigurationError("Specified configuration file not"
                                 " found: {0}".format(config_file))
    if not os.path.isfile(config_file):
        raise ConfigurationError("Specified configuration file is not"
                                 " a file: {0}".format(config_file))


def update_configuration(config, update_dict):
    """Update configuration with new values passed as dictionary.

    :return: new configuration ``namedtuple``
    """
    tmp = config._asdict()
    tmp.update(update_dict)
    return Configuration(**tmp)


def load_configuration(cmd_line_file):
    """Load configurations from the more generic to the
    more specific configuration file. The latter configurations
    override the previous one.
    If a file is specified from command line, it is considered
    the most specific.

    :return: configuration ``namedtuple``
    """
    config_files_hierarchy = [expand_path(c) for c in CONFIG_FILES_PATH]

    if cmd_line_file is not None:
        _validate_config_file(cmd_line_file)
        config_files_hierarchy.append(cmd_line_file)

    config = configparser.SafeConfigParser()
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
