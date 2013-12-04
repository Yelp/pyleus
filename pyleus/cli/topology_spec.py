"""Storm topologies specifications logic.

Note: this module is coupled with the pyleus_topology.yaml file format
and with the Spec classes in the Java code. Any change to this class should
be propagated to this two resources, too.
"""
from __future__ import absolute_import

import copy

from pyleus.exception import InvalidTopologyError


def _as_set(obj):
    return None if obj is None else set(obj)


def _as_list(obj):
    return None if obj is None else list(obj)


class TopologySpec(object):
    """Topology level specification class"""

    def __init__(self, specs):
        """Convert the specs dictionary coming from the yaml file into
        attributes and perform validation at topology level."""

        if _as_set(specs) != set(["name", "topology"]):
            raise InvalidTopologyError(
                "Each topology must specify tags 'name' and 'topology'"
                "only. Found: {1}".format(self.name, _as_list(specs)))

        self.name = specs["name"]
        self.topology = []

        for component in specs["topology"]:
            if "spout" in component:
                self.topology.append(SpoutSpec(component["spout"]))
            elif "bolt" in component:
                self.topology.append(BoltSpec(component["bolt"]))
            else:
                raise InvalidTopologyError(
                    "Unknown tag. Allowed:'bolt' and 'spout'. Found: {0}"
                    .format(_as_list(specs["topology"])))

    def verify_groupings(self):
        """Verify that the groupings specified in the yaml file match
        with all the other specs.
        """
        topology_out_fields = {}
        for component in self.topology:
            topology_out_fields[component.name] = component.output_fields

        for component in self.topology:
            if (isinstance(component, BoltSpec) and
                    component.groupings is not None):
                component.verify_groupings(topology_out_fields)

    def asdict(self):
        """Return a copy of the object as a dictionary"""
        dict_object = copy.deepcopy(self.__dict__)
        dict_object["topology"] = [
            component.asdict() for component in self.topology]
        return dict_object


class ComponentSpec(object):
    """Base class for Storm component specifications"""

    COMPONENT = "component"

    KEYS_LIST = [
        "name", "module", "tick_freq_secs", "parallelism_hint", "options",
        "output_fields", "groupings"]

    def __init__(self, specs):
        """Convert a component specs dictionary coming from the yaml file into
        attributes and perform validation at the component level."""
        if specs is None:
            raise InvalidTopologyError(
                "[{0}] Empty components are not allowed. At least 'name'"
                " and 'module' must be specified".format(self.COMPONENT))

        if not "name" in specs:
            raise InvalidTopologyError(
                "[{0}] Tag not found in yaml file: {1}"
                .format(self.COMPONENT, "name"))
        self.name = specs["name"]

        if not set(self.KEYS_LIST).issuperset(_as_set(specs)):
            raise InvalidTopologyError(
                "[{0}] These tags are not allowed: {1}"
                .format(self.name, _as_list(specs) - set(self.KEYS_LIST)))

        if not "module" in specs:
            raise InvalidTopologyError(
                "[{0}] Tag not found in yaml file: {1}"
                .format(self.name, "module"))
        self.module = specs["module"]

        self.tick_freq_secs = specs.get("tick_freq_secs", None)
        self.parallelism_hint = specs.get("parallelism_hint", None)
        # These two are not currently specified in the yaml file
        self.options = specs.get("options", None)
        self.output_fields = specs.get("output_fields", None)

    def update_from_module(self, specs):
        """Update the component specs wyith the ones coming from the python
        module and perform some additional validation.
        """
        if _as_set(specs) != set(["output_fields", "options"]):
            raise InvalidTopologyError(
                "[{0}] Python class should specify attributes 'output_fields'"
                " and 'options'. Found: {1}. Are you inheriting from Bolt or"
                " Spout?".format(self.name, specs))

        self.output_fields = specs["output_fields"]
        if _as_set(specs["options"]) != _as_set(self.options):
            raise InvalidTopologyError(
                "[{0}] Options mismatch. Python class: {1}. Yaml file: {2}"
                .format(self.name,
                        _as_list(specs["options"]),
                        _as_list(self.options)))

    def asdict(self):
        """Return a copy of the object as a dictionary"""
        return {self.COMPONENT: copy.deepcopy(self.__dict__)}


class BoltSpec(ComponentSpec):
    """Bolt specifications class"""

    COMPONENT = "bolt"

    def __init__(self, specs):
        """Bolt specific initialization. Bolts may have a grouping section"""
        super(BoltSpec, self).__init__(specs)

        if "groupings" in specs:
            self.groupings = specs["groupings"]

    def _stream_exists(self, stream, group_type, topo_out_fields):
        """If stream does not exist in the topology specs, raise an error"""
        if stream not in topo_out_fields:
            raise InvalidTopologyError(
                "[{0}] [{1}] Unknown stream: {2}"
                .format(self.name, group_type, stream))

    def verify_groupings(self, topo_out_fields):
        """Verify that the groupings specified in the yaml file for that
        component match with all the other specs.
        """
        for group in self.groupings:
            if len(group) != 1:
                raise InvalidTopologyError(
                    "[{0}] Each grouping element must specify one and only"
                    " one type. Found: {1}"
                    .format(self.name, group.keys()))
            group_type = group.keys()[0]

            if (group_type == "globalGrouping" or
                    group_type == "shuffleGrouping"):
                stream = group[group_type]
                self._stream_exists(stream, group_type, topo_out_fields)

            elif group_type == "fieldsGrouping":
                fields_dict = group["fieldsGrouping"]

                if _as_set(fields_dict) != set(["component", "fields"]):
                    raise InvalidTopologyError(
                        "[{0}] [{1}] Must specify tags 'component' and"
                        " 'fields' only. Found: {2}".format(
                            self.name, group_type,
                            _as_list(fields_dict)))

                stream = fields_dict["component"]
                self._stream_exists(stream, group_type, topo_out_fields)

                fields = fields_dict["fields"]
                if fields is None:
                    raise InvalidTopologyError(
                        "[{0}] [{1}] Must specify at least one field."
                        .format(self.name, group_type))

                for field in fields:
                    if field not in topo_out_fields[stream]:
                        raise InvalidTopologyError(
                            "[{0}] [{1}] Stream {2} does not have field:"
                            " {3}.".format(
                                self.name, group_type, stream, field))

            else:
                raise InvalidTopologyError(
                    "[{0}] Unkonown grouping type. Allowed:"
                    " 'globalGrouping', 'shuffleGrouping','fieldsGrouping'"
                    ". Found: {1}".format(self.name, group_type))


class SpoutSpec(ComponentSpec):
    """Spout specifications class"""

    COMPONENT = "spout"

    def update_from_module(self, specs):
        """Specific spout validation. Spouts must have output fields."""
        super(SpoutSpec, self).update_from_module(specs)
        if self.output_fields is None:
            raise InvalidTopologyError(
                "[{0}] Spout must have 'output_fields' specified in its Python"
                " module".format(self.name))
