"""Command-line interface to Pyleus. Routes to differents sub-commands modules
based on arguments provided.
"""
from __future__ import absolute_import

import argparse
import logging

from pyleus import __version__
from pyleus.cli.commands.build_subcommand import BuildSubCommand
from pyleus.cli.commands.list_subcommand import ListSubCommand
from pyleus.cli.commands.local_subcommand import LocalSubCommand
from pyleus.cli.commands.submit_subcommand import SubmitSubCommand
from pyleus.cli.commands.kill_subcommand import KillSubCommand

SUB_COMMAND_CLASSES = [
    BuildSubCommand,
    ListSubCommand,
    LocalSubCommand,
    SubmitSubCommand,
    KillSubCommand,
]


def main():
    parser = argparse.ArgumentParser(
        description="Python interface and tools for Storm",
        add_help=False)
    parser.add_argument(
        "-h", "--help", action="help",
        help="Show this message and exit")
    parser.add_argument(
        "-c", "--config", dest="config_file",
        default=None,
        help="Path to Pyleus configuration file")
    parser.add_argument(
        "-v", "--verbose", dest="verbose",
        default=False, action="store_true",
        help="Verbose output")
    parser.add_argument(
        "--version", action="version",
        version="%(prog)s {0}".format(__version__),
        help="Show version number and exit")

    subparsers = parser.add_subparsers(
        title="commands",
        metavar="COMMAND")

    for cls in SUB_COMMAND_CLASSES:
        cls().init_subparser(subparsers)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)
    args.func(args)
