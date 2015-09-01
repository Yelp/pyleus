"""Storm topologies specifications logic.

Note: this module is coupled with the pyleus_topology.yaml file format
and with the Spec classes in the Java code. Any change to this class should
be propagated to this two resources, too.
"""
from __future__ import absolute_import

import copy

from pyleus.exception import InvalidTopologyError
from pyleus.storm import DEFAULT_STREAM
from pyleus.storm.component import SERIALIZERS


def _as_set(obj):
    return set() if obj is None else set(obj)


def _as_list(obj):
    return list() if obj is None else list(obj)


class TopologySpec(object):
    """Topology level specification class."""

    def __init__(self, specs):
        """Convert the specs dictionary coming from the yaml file into
        attributes and perform validation at topology level."""

        if not _as_set(specs).issuperset(set(["name", "topology"])):
            raise InvalidTopologyError(
                "Each topology must specify tags 'name' and 'topology'"
                " Found: {0}".format(_as_list(specs)))

        self.name = specs["name"]
        if "topology_debug" in specs:
            self.topology_debug = specs["topology_debug"]
        if "workers" in specs:
            self.workers = specs["workers"]
        if "ackers" in specs:
            self.ackers = specs["ackers"]
        if "max_spout_pending" in specs:
            self.max_spout_pending = specs["max_spout_pending"]
        if "max_shellbolt_pending" in specs:
            self.max_shellbolt_pending = specs["max_shellbolt_pending"]
        if "message_timeout_secs" in specs:
            self.message_timeout_secs = specs["message_timeout_secs"]
        if "logging_config" in specs:
            self.logging_config = specs["logging_config"]

        if "sleep_spout_wait_strategy_time_ms" in specs:
            self.sleep_spout_wait_strategy_time_ms = specs["sleep_spout_wait_strategy_time_ms"]

        if "worker_childopts_xmx" in specs:
            self.worker_childopts_xmx = specs["worker_childopts_xmx"]

        if "executor_receive_buffer_size" in specs:
            self.executor_receive_buffer_size = specs["executor_receive_buffer_size"]

        if "executor_send_buffer_size" in specs:
            self.executor_send_buffer_size = specs["executor_send_buffer_size"]

        if "transfer_buffer_size" in specs:
            self.transfer_buffer_size = specs["transfer_buffer_size"]

        if "serializer" in specs:
            if specs["serializer"] in SERIALIZERS:
                self.serializer = specs["serializer"]
            else:
                raise InvalidTopologyError(
                    "Unknown serializer. Allowed: {0}. Found: {1}"
                    .format(SERIALIZERS, specs["serializer"]))

        self.requirements_filename = specs.get("requirements_filename")
        self.python_interpreter = specs.get("python_interpreter")

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
        """Return a copy of the object as a dictionary."""
        dict_object = copy.deepcopy(self.__dict__)
        dict_object["topology"] = [
            component.asdict() for component in self.topology]
        return dict_object


class ComponentSpec(object):
    """Base class for Storm component specifications."""

    COMPONENT = "component"

    KEYS_LIST = [
        "name", "type", "module", "tick_freq_secs", "parallelism_hint",
        "options", "output_fields", "groupings", "tasks"]

    def __init__(self, specs):
        """Convert a component specs dictionary coming from the yaml file into
        attributes and perform validation at the component level."""
        if specs is None:
            raise InvalidTopologyError(
                "[{0}] Empty components are not allowed. At least 'name'"
                " and 'module' must be specified".format(self.COMPONENT))

        if "name" not in specs:
            raise InvalidTopologyError(
                "[{0}] Tag not found in yaml file: {1}"
                .format(self.COMPONENT, "name"))
        self.name = specs["name"]

        self.type = specs.get("type", "python")

        if not set(self.KEYS_LIST).issuperset(_as_set(specs)):
            raise InvalidTopologyError(
                "[{0}] These tags are not allowed: {1}"
                .format(self.name,
                        _as_list(_as_set(specs) - set(self.KEYS_LIST))))

        # Optional parameters
        if "tick_freq_secs" in specs:
            self.tick_freq_secs = specs["tick_freq_secs"]
        if "parallelism_hint" in specs:
            self.parallelism_hint = specs["parallelism_hint"]
        if "tasks" in specs:
            self.tasks = specs["tasks"]

        # These two are not currently specified in the yaml file
        self.options = specs.get("options", None)
        self.output_fields = specs.get("output_fields", None)

    def update_from_module(self, specs):
        """Update the component specs with the ones coming from the python
        module and perform some additional validation.
        """
        required_attributes = ["component_type", "output_fields", "options"]
        if _as_set(specs) != set(required_attributes):
            raise InvalidTopologyError(
                "[{0}] Python class should specify attributes 'output_fields'"
                " and 'options'. Found: {1}. Are you inheriting from Bolt or"
                " Spout?".format(self.name, specs))

        if specs["component_type"] != self.COMPONENT:
            raise InvalidTopologyError(
                "[{0}] Component type mismatch. Python class: {1}. Yaml"
                " file: {2}".format(self.name, specs["component_type"],
                                    self.COMPONENT))

        self.output_fields = specs["output_fields"]

        module_opt = _as_set(specs["options"])
        yaml_opt = _as_set(self.options)
        if not module_opt.issuperset(yaml_opt):
            raise InvalidTopologyError(
                "[{0}] Options mismatch. Python class: {1}. Yaml file: {2}"
                .format(self.name,
                        _as_list(specs["options"]),
                        _as_list(self.options)))

    def asdict(self):
        """Return a copy of the object as a dictionary"""
        return {self.COMPONENT: copy.deepcopy(self.__dict__)}


class BoltSpec(ComponentSpec):
    """Bolt specifications class."""

    COMPONENT = "bolt"

    GROUPINGS_LIST = [
        "global_grouping", "shuffle_grouping", "fields_grouping",
        "local_or_shuffle_grouping", "none_grouping", "all_grouping"]

    def __init__(self, specs):
        """Bolt specific initialization. Bolts may have a grouping section."""
        super(BoltSpec, self).__init__(specs)

        if "module" not in specs:
            raise InvalidTopologyError(
                "[{0}] Tag not found in yaml file: {1}"
                .format(self.name, "module"))
        self.module = specs["module"]

        if "groupings" in specs:
            self.groupings = []
            for grouping in specs["groupings"]:
                self.groupings.append(self._expand_grouping(grouping))

    def _expand_grouping(self, group):
        """Normalize the groupings specified in the yaml file for that
        component.
        """
        if len(group) != 1:
            raise InvalidTopologyError(
                "[{0}] Each grouping element must specify one and only"
                " one type. Found: {1}"
                .format(self.name, group.keys()))

        group_type = list(group.keys())[0]

        if (group_type not in self.GROUPINGS_LIST):
            raise InvalidTopologyError(
                "[{0}] Unknown grouping type. Allowed: {1}. Found: {2}"
                .format(self.name, self.GROUPINGS_LIST, group_type))

        group_spec = group[group_type]
        # only the name of the component has been specified
        if isinstance(group[group_type], str):
            group_spec = {
                "component": group_spec,
                "stream": DEFAULT_STREAM,
            }

        # specified component tag, but not stream
        elif "stream" not in group_spec:
            group_spec["stream"] = DEFAULT_STREAM

        return {group_type: group_spec}

    def _verify_grouping_format(self, group_type, group_spec):
        """Verify grouping format based on the kind of grouping."""
        if group_type in (
                "global_grouping",
                "shuffle_grouping",
                "local_or_shuffle_grouping",
                "none_grouping",
                "all_grouping"):
            if _as_set(group_spec) != set(["component", "stream"]):
                raise InvalidTopologyError(
                    "[{0}] [{1}] Unrecognized format: {2}".format(
                        self.name, group_type,
                        _as_list(group_spec)))

        elif group_type == "fields_grouping":
            if (_as_set(group_spec) !=
                    set(["component", "stream", "fields"])):
                raise InvalidTopologyError(
                    "[{0}] [{1}] Unrecognized format: {2}".format(
                        self.name, group_type,
                        _as_list(group_spec)))

            fields = group_spec["fields"]
            if fields is None:
                raise InvalidTopologyError(
                    "[{0}] [{1}] Must specify at least one field."
                    .format(self.name, group_type))

    def _stream_exists(self, component, stream, group_type, topo_out_fields):
        """If stream does not exist in the topology specs, raise an error."""
        if (component not in topo_out_fields or
                stream not in topo_out_fields[component]):
            raise InvalidTopologyError(
                "[{0}] [{1}] Unknown stream: [{2}] [{3}]"
                .format(self.name, group_type, component, stream))

    def _verify_grouping_input(self, group_type, group_spec, topo_out_fields):
        """Verify that grouping input streams and fields exist within the
        topology.
        """
        component = group_spec["component"]
        stream = group_spec["stream"]
        self._stream_exists(
            component,
            stream,
            group_type,
            topo_out_fields)

        if "fields" in group_spec:
            fields = group_spec["fields"]

            for field in fields:
                if field not in topo_out_fields[component][stream]:
                    raise InvalidTopologyError(
                        "[{0}] [{1}] Stream {2} does not have field:"
                        " {3}.".format(
                            self.name, group_type, stream, field))

    def verify_groupings(self, topo_out_fields):
        """Verify that the groupings specified in the yaml file for that
        component match with all the other specs.
        """
        for group in self.groupings:
            group_type = list(group.keys())[0]
            group_spec = group[group_type]

            self._verify_grouping_format(group_type, group_spec)
            self._verify_grouping_input(group_type, group_spec,
                                        topo_out_fields)


class SpoutSpec(ComponentSpec):
    """Spout specifications class."""

    COMPONENT = "spout"

    def __init__(self, specs):
        """Spout specific initialization."""
        super(SpoutSpec, self).__init__(specs)

        if self.type == "python":
            if "module" not in specs:
                raise InvalidTopologyError(
                    "[{0}] Tag not found in yaml file: {1}"
                    .format(self.name, "module"))
            self.module = specs["module"]

        if self.type == "kafka":
            self.output_fields = {DEFAULT_STREAM: ["message"]}

    def update_from_module(self, specs):
        """Specific spout validation. Spouts must have output fields."""
        super(SpoutSpec, self).update_from_module(specs)

        if self.output_fields is None:
            raise InvalidTopologyError(
                "[{0}] Spout must have 'output_fields' specified in its Python"
                " module".format(self.name))
