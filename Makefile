.DELETE_ON_ERROR:

all: sdist bdist_wheel topology_builder

sdist:
	python setup.py sdist

bdist_wheel:
	python setup.py bdist_wheel

test:
	tox

topology_builder:
	make -C topology_builder/

clean:
	rm -rf build/ dist/ pyleus.egg-info/ .tox/
	find . -name '*.pyc' -delete
	make -C topology_builder/ clean
	rm -f pyleus/pyleus-base.jar
	rm -rf docs/build/

docs:
	tox -e docs

.PHONY: all sdist bdist_wheel test clean docs
