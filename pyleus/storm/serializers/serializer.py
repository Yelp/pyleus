"""Base class for all serialziers used by Storm component. Please note that for
each serializer a Java counterpart need to be built.
"""


class Serializer(object):

    def __init__(self, input_stream, output_stream):
        self._input_stream = input_stream
        self._output_stream = output_stream

    def read_msg(self):
        """Return the dictionary message received on the input stream.
        raises: StormWentAwayError if EOF is reached."""
        raise NotImplementedError

    def send_msg(self, msg_dict):
        """Serialize a message dictionary and write it to the output stream."""
        raise NotImplementedError
