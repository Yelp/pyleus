try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from .storm import SimpleBolt


class JSONFieldsBolt(SimpleBolt):

    def extract_fields(self, json_dict):
        """Implement in a subclass to extract the desired fields from
        json_dict

        Return a list of values, or None to emit nothing.
        """
        raise NotImplementedError()

    def process_tuple(self, tup):
        line, = tup.values
        json_dict = json.loads(line)

        fields = self.extract_fields(json_dict)
        if fields is None:
            return

        self.emit(fields, anchors=[tup])
