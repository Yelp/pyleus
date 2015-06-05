"""Module containing the base class for all pyleus components and a wrapper
class around Storm configurations.
"""
from __future__ import absolute_import

import argparse
from collections import deque
import logging
import logging.config
import os
import sys
import traceback

try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from pyleus.storm import DEFAULT_STREAM
from pyleus.storm import LOG_TRACE
from pyleus.storm import LOG_DEBUG
from pyleus.storm import LOG_INFO
from pyleus.storm import LOG_WARN
from pyleus.storm import LOG_ERROR
from pyleus.storm import StormTuple
from pyleus.storm.serializers.msgpack_serializer import MsgpackSerializer
from pyleus.storm.serializers.json_serializer import JSONSerializer


# Please keeep in sync with java TopologyBuilder
DESCRIBE_OPT = "--describe"
COMPONENT_OPTIONS_OPT = "--options"
PYLEUS_CONFIG_OPT = "--pyleus-config"

DEFAULT_LOGGING_CONFIG_PATH = "pyleus_logging.conf"

JSON_SERIALIZER = "json"
MSGPACK_SERIALIZER = "msgpack"
SERIALIZERS = {
    JSON_SERIALIZER: JSONSerializer,
    MSGPACK_SERIALIZER: MsgpackSerializer,
}


log = logging.getLogger(__name__)


def _is_namedtuple(obj):
    return (type(obj) is type and
            issubclass(obj, tuple) and
            hasattr(obj, "_fields"))


def _serialize(obj):
    """Given a list, a tuple or a namedtuple, return it as a list. In case of
    None, simply return None.
    """
    if obj is None:
        return None
    # obj is a namedtuple "class"
    elif _is_namedtuple(obj):
        return list(obj._fields)
    # obj is a list or a tuple
    return list(obj)


def _expand_output_fields(obj):
    """Expand all allowed notations for defining OUTPUT_FIELDS into the
    extended one.
    """
    # if single-stream notation
    if not isinstance(obj, dict):
        return {DEFAULT_STREAM: _serialize(obj)}

    # if multiple-streams notation
    for key, value in obj.items():
        obj[key] = _serialize(value)
    return obj


class StormConfig(dict):
    """Add some convenience properites to a configuration ``dict`` from Storm.
    You can access Storm configuration dictionary within a component through
    ``self.conf``.
    """

    def __init__(self, conf):
        super(StormConfig, self).__init__()
        self.update(conf)

    @property
    def tick_tuple_freq(self):
        """Helper property to access the value of tick tuple frequency stored
        in Storm configuration.

        :return: tick tuple frequency for the component
        :rtype: ``float`` or ``None``

        .. note::
           Bolts not specifying tick tuple frequency default to ``None``,
           while spouts are not supposed to use tick tuples at all.
        """
        return self.get("topology.tick.tuple.freq.secs")


class Component(object):
    """Base class for all pyleus components."""

    COMPONENT_TYPE = None # One of "bolt", "spout"

    #: ``list`` or ``dict`` of output fields for the component.
    #:
    #: .. note:: Specify in subclass.
    #:
    #: .. seealso:: :ref:`groupings`
    OUTPUT_FIELDS = None

    #: ``list`` of user-defined options for the component.
    #:
    #: .. note:: Specify in subclass.
    OPTIONS = None

    # Populated in Component.run()

    #: ``dict`` containing options passed to component in the yaml definition
    #: file.
    options = None

    #: :class:`~.StormConfig` containing the Storm configuration for the
    #: component.
    conf = None

    #: ``dict`` containing the Storm context for the component.
    context = None

    pyleus_config = None

    def __init__(self, input_stream=None, output_stream=None):
        """The Storm component will parse the command line in order
        to figure out if it has been queried for a description or for
        actually running."""
        super(Component, self).__init__()

        if input_stream is None:
            input_stream = sys.stdin

        if output_stream is None:
            output_stream = sys.stdout

        self._input_stream = input_stream
        self._output_stream = output_stream

        self._pending_commands = deque()
        self._pending_taskids = deque()

        self._serializer = None

    def describe(self):
        """Print to stdout a JSON description of the component.

        The java TopologyBuilder will use the JSON descrption for topology
        cofiguration and validation.
        """

        print(json.dumps({
            "component_type": self.COMPONENT_TYPE,
            "output_fields": _expand_output_fields(self.OUTPUT_FIELDS),
            "options": _serialize(self.OPTIONS)}))

    def initialize_logging(self):
        """Load logging configuration file from command line configuration (if
        provided) and initialize logging for the component.
        """
        logging_config_path = self.pyleus_config.get('logging_config_path')
        if logging_config_path:
            logging.config.fileConfig(logging_config_path)
        elif os.path.isfile(DEFAULT_LOGGING_CONFIG_PATH):
            logging.config.fileConfig(DEFAULT_LOGGING_CONFIG_PATH)

    def initialize_serializer(self):
        """Load serializer type from command line configuration and instantiate
        the associated
        :class:`~pyleus.storm.serializers.serializer.Serializer`.
        """
        serializer = self.pyleus_config.get('serializer')
        if serializer in SERIALIZERS:
            self._serializer = SERIALIZERS[serializer](
                self._input_stream, self._output_stream)
        else:
            raise ValueError("Unknown serializer: {0}", serializer)

    def setup_component(self):
        """Storm component setup before execution. It will also
        call the initialization method implemented in the subclass.
        """
        self.conf, self.context = self._init_component()
        self.initialize()

    def initialize(self):
        """Called after component has been launched, but before processing any
        tuples. You can use this method to setup your component.

        .. note:: Implement in subclass.
        """
        pass

    def run(self):
        """Entry point for the component running logic.

        Forgetting to call it as following will prevent the topology from
        running.

        :Example:
         .. code-block:: python

            if __name__ == '__main__':
                MyComponent().run()
        """
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(DESCRIBE_OPT, action="store_true", default=False)
        parser.add_argument(COMPONENT_OPTIONS_OPT, default=None)
        parser.add_argument(PYLEUS_CONFIG_OPT, default=None)
        args = parser.parse_args()

        if args.describe:
            self.describe()
            return

        self.options = json.loads(args.options) if args.options else {}
        self.pyleus_config = json.loads(args.pyleus_config) \
            if args.pyleus_config else {}

        try:
            self.initialize_logging()
            self.initialize_serializer()
            self.setup_component()
            self.run_component()
        except:
            log.exception("Exception in {0}.run".format(self.COMPONENT_TYPE))
            self.error(traceback.format_exc())

    def run_component(self):
        """Run the main loop of the component. Implemented in Bolt and
        Spout subclasses.
        """
        raise NotImplementedError

    def _msg_is_command(self, msg):
        """Storm differentiates between commands and taskids by whether the
        message is a ``dict`` or ``list``.
        """
        return isinstance(msg, dict)

    def _msg_is_taskid(self, msg):
        """..seealso::  :meth:`~._msg_is_command`"""
        return isinstance(msg, list)

    def read_command(self):
        """Return the next command from the input stream, whether from the
        _pending_commands queue or the stream directly if the queue is empty.

        In that case, queue any taskids which are received until the next
        command comes in.
        """
        if self._pending_commands:
            return self._pending_commands.popleft()

        msg = self._serializer.read_msg()

        while self._msg_is_taskid(msg):
            self._pending_taskids.append(msg)
            msg = self._serializer.read_msg()

        return msg

    def read_taskid(self):
        """Like :meth:`~.read_command`, but returns the next taskid and queues
        any commands received while reading the input stream.
        """
        if self._pending_taskids:
            return self._pending_taskids.popleft()

        msg = self._serializer.read_msg()

        while self._msg_is_command(msg):
            self._pending_commands.append(msg)
            msg = self._serializer.read_msg()

        return msg

    def read_tuple(self):
        """Read and parse a command into a StormTuple object."""
        cmd = self.read_command()
        return StormTuple(
            cmd['id'], cmd['comp'], cmd['stream'], cmd['task'], cmd['tuple'])

    def _create_pidfile(self, pid_dir, pid):
        """Create a file based on pid used by Storm to watch over the Python
        process.
        """
        open(os.path.join(pid_dir, str(pid)), 'a').close()

    def _init_component(self):
        """Receive the setup_info dict from the Storm task and report back with
        our pid; also touch a pidfile in the pidDir specified in setup_info.
        """
        setup_info = self._serializer.read_msg()

        pid = os.getpid()
        self._serializer.send_msg({'pid': pid})
        self._create_pidfile(setup_info['pidDir'], pid)

        return StormConfig(setup_info['conf']), setup_info['context']

    def send_command(self, command, opts_dict=None):
        """Merge command with options and send the message through
        :class:`~pyleus.storm.serializers.serializer.Serializer`
        """
        if opts_dict is not None:
            command_dict = dict(opts_dict)
            command_dict['command'] = command
        else:
            command_dict = dict(command=command)

        self._serializer.send_msg(command_dict)

    def log(self, msg, level=LOG_INFO):
        """Send a log message.

        :param msg: log message
        :type msg: ``str``
        :param level:
         log levels defined as constants in :mod:`pyleus.storm`.
         Allowed: ``LOG_TRACE``, ``LOG_DEBUG``, ``LOG_INFO``, ``LOG_WARN``,
         ``LOG_ERROR``. Default: ``LOG_INFO``
        :type stream: ``int``
        """
        self.send_command('log', {
            'msg': msg,
            'level': level,
        })

    def log_trace(self, msg):
        """Send a log message with level LOG_TRACE.

        :param msg: log message
        :type msg: ``str``
        """
        self.log(msg, level=LOG_TRACE)

    def log_debug(self, msg):
        """Send a log message with level LOG_DEBUG.

        :param msg: log message
        :type msg: ``str``
        """
        self.log(msg, level=LOG_DEBUG)

    def log_info(self, msg):
        """Send a log message with level LOG_INFO.

        :param msg: log message
        :type msg: ``str``
        """
        self.log(msg, level=LOG_INFO)

    def log_warn(self, msg):
        """Send a log message with level LOG_WARN.

        :param msg: log message
        :type msg: ``str``
        """
        self.log(msg, level=LOG_WARN)

    def log_error(self, msg):
        """Send a log message with level LOG_ERROR.

        :param msg: log message
        :type msg: ``str``
        """
        self.log(msg, level=LOG_ERROR)

    def error(self, msg):
        """Send an error message.

        :param msg: error message
        :type msg: ``str``
        """
        self.send_command('error', {
            'msg': msg,
        })
