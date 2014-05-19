from pyleus.storm import StormTuple, is_tick


class TestStormUtilFunctions(object):

    def test_is_tick_true(self):
        tup = StormTuple(None, '__system', '__tick', None, None)
        assert is_tick(tup)

    def test_is_tick_false(self):
        tup = StormTuple(None, '__system', None, None, None)
        assert not is_tick(tup)
        tup = StormTuple(None, None, '__tick', None, None)
        assert not is_tick(tup)
