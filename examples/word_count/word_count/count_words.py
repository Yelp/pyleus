from collections import defaultdict
from collections import namedtuple

from pyleus.storm import SimpleBolt


Counter = namedtuple("Counter", "word count")


class CountWordsBolt(SimpleBolt):

    OUTPUT_FIELDS = Counter

    def initialize(self, conf, context, options=None):
        self.words = defaultdict(int)

    def process_tuple(self, tup):
        word, = tup.values
        self.words[word] += 1
        self.emit((word, self.words[word]), anchors=[tup])


if __name__ == '__main__':
    CountWordsBolt().run()
