from __future__ import absolute_import

import logging

from random import choice
import time

try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from pyleus.storm import Spout


log = logging.getLogger('requests_generator')

FIRST = (
    "www.ninjacorp.com",
    "www.ninjacorp.com",
    "www.ninjacorp.com",
    "www.ninjacorp.com",
    "www.ninjacorp.it",
    "www.ninjacorp.jp",)

SECOND = ("hidden", "hidden", "deadly", "stale", "cute",)

THIRD = (
    "ninja-cat.php",
    "ninja-cat.php",
    "ninja-cat.php",
    "ninja-cat.php",
    "evil-dog.php",
    "evil-dog.php",
    "wafel-shuriken.html",)

PROTOCOLS = ("HTTP", "HTTP", "HTTP", "HTTP", "FTP")


class RequestsGeneratorSpout(Spout):
    OUTPUT_FIELDS = ["request"]

    def next_tuple(self):
        time.sleep(0.001)
        request = {
            "timestamp": time.time(),
            "request": {
                "protocol": choice(PROTOCOLS),
                "url": "{0}/{1}-{2}".format(
                    choice(FIRST), choice(SECOND), choice(THIRD))
            }
        }
        log.debug(request)
        self.emit((json.dumps(request),))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/top_urls_requests_generator.log',
        filemode='a',
    )

    RequestsGeneratorSpout().run()
