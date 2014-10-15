from __future__ import absolute_import

import os
import sys

__version__ = '0.2'

BASE_JAR = "pyleus-base.jar"
BASE_JAR_INSTALL_DIR = "share/pyleus"
BASE_JAR_PATH = os.path.join(sys.prefix, BASE_JAR_INSTALL_DIR, BASE_JAR)
