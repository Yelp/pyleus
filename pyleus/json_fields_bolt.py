"""JSON-specialized Bolt component."""
from __future__ import absolute_import

import logging

try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from .storm import SimpleBolt

log = logging.getLogger(__name__)


class JSONFieldsBolt(SimpleBolt):
    """JSON-specialized SimpleBolt abstracting JSON-parsing
    and tuple processing from the actual application logic.
    """

    def extract_fields(self, json_dict):
        """Implement in a subclass to extract the desired fields from
        json_dict.

        :param json_dict: JSON object representing the input tuple value
        :type json_dict: ``dict``

        :return: a list of values, or ``None`` to emit nothing.
        :rtype: ``list`` or ``None``

        .. note:: Implement in subclass.
        """
        raise NotImplementedError()

    def process_tuple(self, tup):
        """Extract JSON representation of incoming tuple value and emit based
        on the result of :meth:`~.extract_fields`.

        .. note:: Emitted tuples are automatically anchored.
        """
        line, = tup.values
        json_dict = json.loads(line)

        fields = self.extract_fields(json_dict)
        if fields is None:
            return

        self.emit(fields, anchors=[tup])
