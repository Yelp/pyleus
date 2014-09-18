"""JSON implementation of Pyleus serializer"""

try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from pyleus.storm import StormWentAwayError
from pyleus.storm.serializers.serializer import Serializer


class JSONSerializer(Serializer):

    def read_msg(self):
        """The Storm multilang protocol consists of JSON messages followed by
        a newline and "end\n".
        """
        lines = []
        while True:
            line = self._input_stream.readline()
            if not line:
                # Handle EOF, which usually means Storm went away
                raise StormWentAwayError()

            line = line.strip()

            if line == "end":
                break

            lines.append(line)

        msg_str = '\n'.join(lines)
        return json.loads(msg_str)

    def send_msg(self, msg_dict):
        """Serialize to JSON a message dictionary and write it to the output
        stream, followed by a newline and "end\n".
        """
        self._output_stream.write(json.dumps(msg_dict) + '\nend\n')
        self._output_stream.flush()
