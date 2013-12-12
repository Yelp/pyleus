from __future__ import absolute_import

import logging
from urllib import urlencode

from httplib2 import Http
import jinja2

from pyleus.storm import SimpleBolt

from .top_ten_intermediate import Query

log = logging.getLogger('serve_bolt')

RESULTS_TRACKER_URL = "http://dev9-devc.dev.yelpcorp.com:8123/result/lucy_slow_queries-stage"

TEMPLATE = jinja2.Template("""\
Top Ten Slowest Search Queries:

{%- for query, n, duration, max_duration in top_ten %}
    {{ loop.index }}. {{ "%.2f" % duration }}ms (N={{ n }}, max={{ "%.2f" % max_duration }}ms) : "{{ query.desc }}", {{ query.loc }}
{%- endfor %}

Results shown are the top ten slowest queries over the past ten minutes.
""")


class ServeBolt(SimpleBolt):

    def process_tuple(self, tup):
        top_ten_raw = tup.values[0]

        # Convert query tuples into Query namedtuples
        top_ten = [
            (Query(*query), n, duration, max_duration)
            for query, n, duration, max_duration in top_ten_raw
        ]

        result = self._render_result(top_ten)
        self._submit_result(result)

    def _render_result(self, top_ten):
        return TEMPLATE.render(top_ten=top_ten)

    def _submit_result(self, result):
        data = dict(result=result)
        Http().request(RESULTS_TRACKER_URL, 'POST', urlencode(data), headers={'Content-Type': "application/x-www-form-urlencoded"})


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/nail/tmp/serve.log',
        filemode='a',
    )

    ServeBolt().run()
