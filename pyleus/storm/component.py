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
from pyleus.storm import StormTuple
from pyleus.storm.serializers.msgpack_serializer import MsgpackSerializer
from pyleus.storm.serializers.json_serializer import JSONSerializer


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
    if obj is None:
        return None
    # obj is a namedtuple "class"
    elif _is_namedtuple(obj):
        return list(obj._fields)
    # obj is a list or a tuple
    return list(obj)


def _expand_output_fields(obj):
    # if single-stream notation
    if not isinstance(obj, dict):
        return {DEFAULT_STREAM: _serialize(obj)}

    # if multiple-streams notation
    for key, value in obj.items():
        obj[key] = _serialize(value)
    return obj


class StormConfig(dict):
    """Add some convenience properites to a conf dict from Storm."""

    def __init__(self, conf):
        super(StormConfig, self).__init__()
        self.update(conf)

    @property
    def tick_tuple_freq(self):
        """Return the tick tuple frequency for the component.

        Note: bolts that not specify a tick tuple frequency default to null,
        while for spouts are not supposed to use tick tuples.
        """
        return self.get("topology.tick.tuple.freq.secs")


class Component(object):

    COMPONENT_TYPE = None # One of "bolt", "spout"
    OUTPUT_FIELDS = None
    OPTIONS = None

    # Populated in Component.run()
    options = None
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
        """Print to stdout a JSON descrption of the component.

        The java code will use the JSON descrption for topology
        cofiguration and validation.
        """

        print json.dumps({
            "component_type": self.COMPONENT_TYPE,
            "output_fields": _expand_output_fields(self.OUTPUT_FIELDS),
            "options": _serialize(self.OPTIONS)})

    def initialize_logging(self):
        logging_config_path = self.pyleus_config.get('logging_config_path')
        if logging_config_path:
            logging.config.fileConfig(logging_config_path)
        elif os.path.isfile(DEFAULT_LOGGING_CONFIG_PATH):
            logging.config.fileConfig(DEFAULT_LOGGING_CONFIG_PATH)

    def initialize_serializer(self):
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
        """Called after component has been launched, but before processing
        any tuples. Implement in subclass.
        """
        pass

    def run(self):
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
        except Exception as e:
            log.exception("Exception in {0}.run".format(self.COMPONENT_TYPE))
            self.error(traceback.format_exc(e))

    def run_component(self):
        """Run the main loop of the component. Implemented in the Bolt and
        Spout subclasses.
        """
        raise NotImplementedError

    def _msg_is_command(self, msg):
        """Storm differentiates between commands and taskids by whether the
        message is a dict or list.
        """
        return isinstance(msg, dict)

    def _msg_is_taskid(self, msg):
        """See _msg_is_command()"""
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
        """Like read_command(), but returns the next taskid and queues any
        commands received while reading the input stream.
        """
        if self._pending_taskids:
            return self._pending_taskids.popleft()

        msg = self._serializer.read_msg()

        while self._msg_is_command(msg):
            self._pending_commands.append(msg)
            msg = self._serializer.read_msg()

        return msg

    def read_tuple(self):
        """Read and parse a command into a StormTuple object"""
        cmd = self.read_command()
        return StormTuple(
            cmd['id'], cmd['comp'], cmd['stream'], cmd['task'], cmd['tuple'])

    def _create_pidfile(self, pid_dir, pid):
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
        if opts_dict is not None:
            command_dict = dict(opts_dict)
            command_dict['command'] = command
        else:
            command_dict = dict(command=command)

        self._serializer.send_msg(command_dict)

    def log(self, msg):
        self.send_command('log', {
            'msg': msg,
        })

    def error(self, msg):
        self.send_command('error', {
            'msg': msg,
        })
