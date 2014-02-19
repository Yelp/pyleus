.DELETE_ON_ERROR:

all:
	echo >&2 "Must specify target."

test:
	cd pyleus; tox -c tox-yelp.ini

.PHONY: all test test-pyleus test-pyleus-util test-storm-util
