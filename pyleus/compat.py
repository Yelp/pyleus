import sys

if sys.version_info[0] < 3:
    from cStringIO import StringIO
    BytesIO = StringIO
else:
    from io import BytesIO
    from io import StringIO

_ = BytesIO  # pyflakes
_ = StringIO  # pyflakes
