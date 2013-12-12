import cgi
from collections import namedtuple
import logging
import urlparse

from pyleus.json_fields_bolt import JSONFieldsBolt

log = logging.getLogger('fields_bolt')

Fields = namedtuple('Fields', "timestamp query_desc query_loc duration")


class FieldsBolt(JSONFieldsBolt):

    OUTPUT_FIELDS = Fields

    def _get_query_loc_from_request_uri(self, request_uri):
        parsed = urlparse.urlparse(request_uri)
        fields = cgi.parse_qs(parsed.query) # returns list

        if 'find_loc' not in fields:
            log.debug("find_loc not in request_uri")
            return None

        return fields['find_loc'][0]

    def extract_fields(self, json_dict):
        log.debug(json_dict)
        timestamp = json_dict['start_time']
        query_desc = json_dict['extra']['query']['query']

        request_uri = json_dict['extra']['query']['request_uri']
        if not request_uri:
            log.debug("request_uri is null")
            return None

        query_loc = self._get_query_loc_from_request_uri(request_uri)
        if not query_loc:
            return None

        duration = dict(json_dict['checkpoints'])['done']

        return timestamp, query_desc, query_loc, duration


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/nail/tmp/fields.log',
        filemode='a',
    )

    FieldsBolt().run()
