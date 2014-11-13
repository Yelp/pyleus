import logging
import time

from pyleus.storm import Spout

log = logging.getLogger("logging_example.line_spout")


class LineSpout(Spout):

    OUTPUT_FIELDS = ['timestamp']

    INTERVAL = 5 # seconds

    last_emit = None

    def initialize(self):
        self.last_emit = time.time()

    def next_tuple(self):
        now = time.time()
        if now > self.last_emit + self.INTERVAL:
            self.emit((now,))
            log.info("Emitted: %r", now)
            self.last_emit = now
        else:
            time.sleep(0.100)


if __name__ == '__main__':
    LineSpout().run()
