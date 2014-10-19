"""This module is used only by pyleus.cli.build._remove_pyleus_base_jar to
determine the value of BASE_JAR_PATH inside _another_ virtualenv, so it can be
removed to save space.
"""
from __future__ import absolute_import

import pyleus

if __name__ == '__main__':
    print(pyleus.BASE_JAR_PATH)
