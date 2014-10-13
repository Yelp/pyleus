"""Pyleus specific exceptions."""
from __future__ import absolute_import


class PyleusError(Exception):
    """Base class for pyleus specific exceptions."""
    def __str__(self):
        return "[{0}] {1}".format(type(self).__name__,
               ", ".join(str(i) for i in self.args))


class ConfigurationError(PyleusError):
    """Raised when a required configuration value is missing or malformed."""
    pass


class JarError(PyleusError):
    """Raised when a problem occurred with pyleus base jar."""
    pass


class InvalidTopologyError(PyleusError):
    """Raised when topology validation failed. Usually it is due to a syntax
    error or a mispelled name in the topology definition YAML file.
    """
    pass


class VirtualenvError(PyleusError):
    """Raised when an error occurred during virtualenv creation, dependencies
    installation or modules invocation.
    """
    pass


class StormError(PyleusError):
    """Raised when Storm invocation terminates with errors."""
    pass


def command_error_fmt(cmd_name, exception):
    """Format error message given command and exception."""
    return "pyleus {0}: error: {1}".format(cmd_name, str(exception))
