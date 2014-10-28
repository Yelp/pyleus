
try:
    # In python 3.3+ mock got included in the standard library...
    from unittest import mock
except ImportError:
    import mock

from six.moves import builtins, configparser, cStringIO as StringIO
from six import BytesIO

assert mock and builtins and configparser and StringIO and BytesIO
