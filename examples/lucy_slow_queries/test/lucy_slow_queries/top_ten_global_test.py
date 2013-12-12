from __future__ import division

import mock
import testify as T

from lucy_slow_queries.top_ten_intermediate import Query
from lucy_slow_queries.top_ten_global import TopTenGlobalBolt


class TopTenGlobalBoltTestCase(T.TestCase):

    def test_calculate_top_ten(self):
        rankings = {}
        for i in range(3):
            rankings[i] = []
            for j in range(5):
                query_desc = str(15 - (i * 5 + j) - 1)
                duration = i * 5 + j

                query = Query(query_desc, 'foo')
                rankings[i] += [(query, 0, duration)]

        bolt = TopTenGlobalBolt()

        with mock.patch.object(bolt, 'rankings', rankings):
            top_ten = bolt.calculate_top_ten()

        top_ten_query_descs = [
            query.desc
            for query, n, mean_duration in top_ten
        ]

        T.assert_equal(top_ten_query_descs, [str(i) for i in range(10)])
