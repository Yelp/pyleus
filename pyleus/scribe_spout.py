from collections import namedtuple
import logging
import Queue
import sys
import threading
import time

from clog import StreamTailer

from .storm import Spout

log = logging.getLogger('scribe_spout')

ScribeHost = namedtuple('ScribeHost', "host port")

SCRIBE_TAIL_SERVICES = {
    'dev': ScribeHost('scribe-dev.local.yelpcorp.com', 3535),
    'stagea': ScribeHost('scribe-stagea.local.yelpcorp.com', 3535),
    'stageb': ScribeHost('scribe-stageb.local.yelpcorp.com', 3535),
}


class ScribeSpout(Spout):

    QUEUE_SIZE = 1000

    def __init__(self, environment, stream):
        super(ScribeSpout, self).__init__()
        self.environment = environment
        self.stream = stream
        self.queue = Queue.Queue(maxsize=self.QUEUE_SIZE)
        self.producer_thread = None

    def initialize(self, conf, context):
        self.conf = conf
        self.context = context
        log.debug('CONF:%s' % self.conf)
        log.debug('CTX :%s' % self.context)
        self.stream_tailer = StreamTailer(self.stream,
            **SCRIBE_TAIL_SERVICES[self.environment]._asdict())
        self.producer_thread = threading.Thread(target=self.producer)
        self.producer_thread.daemon = True
        self.producer_thread.start()

    def producer(self):
        try:
            for line in self.stream_tailer:
                log.debug("FROM SCRIBE: %s" % line)
                self.queue.put(line, block=True)
        except StopIteration:
            log.error('End of stream "%s" reached', self.stream)

    def ack(self, id):
        log.debug('ACK')

    def fail(self, id):
        log.debug('FAIL')

    def next_tuple(self):
        log.debug('NEXTTUPLE')

        try:
            line = self.queue.get_nowait()
            self.emit((line,))
        except Queue.Empty:
            time.sleep(0.1)

if __name__ == '__main__':
    _, environment, stream = sys.argv[1:]
    ScribeSpout(environment, stream).run()
