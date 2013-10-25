"""Command-line interface to Pyleus. Routes to differents sub-commands modules
based on arguments provided.
"""
from __future__ import absolute_import

import argparse

from pyleus import __version__
from . import jarbuilder


def main():
    parser = argparse.ArgumentParser(
        description="Python layer on top of Storm",
        add_help=False)
    parser.add_argument(
        "-h", "--help", action="help",
        help="Show this message and exit")
    parser.add_argument(
        "-c", "--config", dest="config_file",
        default=None,
        help="Pyleus configuration file")
    parser.add_argument(
        "-v", "--verbose", dest="verbose",
        default=False, action="store_true",
        help="Verbose")
    parser.add_argument(
        "-V", "--version", action="version",
        version="%(prog)s {0}".format(__version__),
        help="Show version number and exit")

    subparsers = parser.add_subparsers(
        title="Commands",
        metavar="COMMAND")
    jarbuilder.add_parser(subparsers)

    args = parser.parse_args()

    args.func(args)
