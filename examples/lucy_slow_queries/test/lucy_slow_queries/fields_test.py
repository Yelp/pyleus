import testify as T

from lucy_slow_queries.fields import FieldsBolt


class FieldsBoltTestCase(T.TestCase):

    def test__get_query_loc_from_request_uri(self):
        request_uri = ("/search/snippet?find_desc=cannabis+club&find_loc=San+J"
            "ose&start=10&parent_request_id=05954d16b2df0208&request_origin=ha"
            "sh&bookmark=true")

        fields_bolt = FieldsBolt()

        query_loc = fields_bolt._get_query_loc_from_request_uri(request_uri)
        T.assert_equal(query_loc, 'San Jose')
