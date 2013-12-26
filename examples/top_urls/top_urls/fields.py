from collections import namedtuple
import logging

from pyleus.json_fields_bolt import JSONFieldsBolt

log = logging.getLogger('fields_bolt')

Fields = namedtuple('Fields', "url timestamp")


class FieldsBolt(JSONFieldsBolt):

    OUTPUT_FIELDS = Fields

    def extract_fields(self, json_dict):
        log.debug(json_dict)

        if json_dict['request']['protocol'] != "HTTP":
            return None

        timestamp = json_dict['timestamp']
        url = json_dict['request']['url']
        log.debug("{0} {1}".format(url, timestamp))
        return url, timestamp


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/top_urls_fields.log',
        filemode='a',
    )

    FieldsBolt().run()
