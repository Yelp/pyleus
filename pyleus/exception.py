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
