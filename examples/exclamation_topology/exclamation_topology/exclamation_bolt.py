from __future__ import absolute_import

import logging

from pyleus.storm import SimpleBolt


log = logging.getLogger('exclamation_bolt')


class ExclamationBolt(SimpleBolt):

    OUTPUT_FIELDS = ["word"]

    def process_tuple(self, tup):
        # tup.values is an iterable, but in this case it contains only one
        # element (word), as declared in test_word_spout.py
        word = tup.values[0] + "!!!"

        log.debug(word)

        # the tuple emitted is anchored to its parent tuple in the Storm
        # reliability tree, so that it will be replayed in case of failure
        self.emit((word,), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/exclamation_bolt.log',
        filemode='a',
    )

    ExclamationBolt().run()
