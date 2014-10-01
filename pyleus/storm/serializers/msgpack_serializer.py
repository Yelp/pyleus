"""Messagepack implementation of Pyleus serializer"""

import os

import msgpack

from pyleus.storm import StormWentAwayError
from pyleus.storm.serializers.serializer import Serializer


def _messages_generator(input_stream):
    unpacker = msgpack.Unpacker()
    while True:
        # f.read(n) on sys.stdin blocks until n bytes are read, causing
        # serializer to hang.
        # os.read(fileno, n) will block if there is nothing to read, but will
        # return as soon as it is able to read at most n bytes.
        line = os.read(input_stream.fileno(), 1024 ** 2)
        if not line:
            # Handle EOF, which usually means Storm went away
            raise StormWentAwayError()
        # As python-msgpack docs suggest, we feed data to the unpacker
        # internal buffer in order to let the unpacker deal with message
        # boundaries recognition and uncomplete messages. In case input ends
        # with a partial message, unpacker raises a StopIteration and will be
        # able to continue after being feeded with the rest of the message.
        unpacker.feed(line)
        for i in unpacker:
            yield i


class MsgpackSerializer(Serializer):

    def __init__(self, input_stream, output_stream):
        super(MsgpackSerializer, self).__init__(input_stream, output_stream)

        self._messages = _messages_generator(self._input_stream)

    def read_msg(self):
        """"Messages are delimited by msgapck itself, no need for Storm
        multilang end line.
        """
        return next(self._messages)

    def send_msg(self, msg_dict):
        """"Messages are delimited by msgapck itself, no need for Storm
        multilang end line.
        """
        msgpack.pack(msg_dict, self._output_stream)
        self._output_stream.flush()
