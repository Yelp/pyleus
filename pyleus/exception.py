"""Pyleus specific exceptions
"""
from __future__ import absolute_import


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


def command_error_fmt(cmd_name, exception):
    """Error message for commands based on exception"""
    return "pyleus {0}: error: {1}".format(cmd_name, str(exception))
