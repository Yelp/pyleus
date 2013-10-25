from __future__ import absolute_import

import sys
import time

from .storm import Spout


class FileSpout(Spout):
    """Emit lines from a file

    NOTE: this spout is unreliable, iterating through the specified file once
    without heeding ACKs or FAILs from the topology.
    """

    def __init__(self, filename):
        super(FileSpout, self).__init__()
        self._filename = filename

    def initialize(self, conf, context):
        self._line_generator = self._get_lines(filename)

    def _get_lines(self, filename):
        with open(filename) as f:
            for line in f:
                yield line

    def next_tuple(self):
        try:
            line = next(self._line_generator)
            self.emit((line,))
        except StopIteration:
            time.sleep(0.010)


if __name__ == '__main__':
    filename = sys.argv[1]
    FileSpout(filename).run()
