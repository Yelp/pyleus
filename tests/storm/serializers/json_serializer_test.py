try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from pyleus.compat import StringIO
from pyleus.testing import mock
from pyleus.storm.serializers.json_serializer import JSONSerializer
from testing.serializer import SerializerTestCase


class TestJSONSerializer(SerializerTestCase):

    INSTANCE_CLS = JSONSerializer

    def test_read_msg_dict(self):
        msg_dict = {
            'hello': "world",
        }

        self.mock_input_stream.readline.side_effect = [
            json.dumps(msg_dict),
            "end",
        ]

        assert self.instance.read_msg() == msg_dict

    def test_read_msg_list(self):
        msg_list = [3, 4, 5]

        self.mock_input_stream.readline.side_effect = [
            json.dumps(msg_list),
            "end",
        ]

        assert self.instance.read_msg() == msg_list

    def test_send_msg(self):
        msg_dict = {
            'hello': "world",
        }

        expected_output = """{"hello": "world"}\nend\n"""

        with mock.patch.object(
                self.instance, '_output_stream', StringIO()) as sio:
            self.instance.send_msg(msg_dict)

        assert sio.getvalue() == expected_output
