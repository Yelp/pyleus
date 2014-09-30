.DELETE_ON_ERROR:

all: sdist bdist_wheel topology_builder

sdist:
	python setup.py sdist

bdist_wheel:
	python setup.py bdist_wheel

test:
	tox -c tox-yelp.ini

topology_builder:
	make -C topology_builder/

clean:
	rm -rf build/ dist/ pyleus.egg-info/ .tox/
	make -C topology_builder/ clean

docs:
	tox -e docs

.PHONY: all sdist bdist_wheel test clean docs
