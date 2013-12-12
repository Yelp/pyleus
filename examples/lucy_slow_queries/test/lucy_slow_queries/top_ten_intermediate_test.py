from __future__ import division

import mock
import testify as T

from lucy_slow_queries.top_ten_intermediate import TopTenIntermediateBolt, Query, Record


class TopTenIntermediateBoltTestCase(T.TestCase):

    def test_calculate_top_ten(self):
        queries = {}
        for i in range(15):
            query_desc = str(15 - i - 1)
            duration = i

            query = Query(query_desc, 'foo')
            record = Record(0, duration)
            queries[query] = [record]

        bolt = TopTenIntermediateBolt()

        with mock.patch.object(bolt, 'queries', queries):
            top_ten = bolt.calculate_top_ten()

        top_ten_query_descs = [
            query.desc
            for query, n, mean_duration in top_ten
        ]

        T.assert_equal(top_ten_query_descs, [str(i) for i in range(10)])

    def test_calculate_top_ten_aggregation(self):
        queries = {
            Query('foo', 'bar'): [Record(0, 17), Record(0, 94)],
        }

        bolt = TopTenIntermediateBolt()

        with mock.patch.object(bolt, 'queries', queries):
            top_ten = bolt.calculate_top_ten()

        _, _, mean = top_ten[0]

        print mean, (17 + 94) / 2, top_ten
        T.assert_equal(mean, (17 + 94) / 2)
