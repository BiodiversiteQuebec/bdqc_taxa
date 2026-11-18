import os
import sys

# Test modules use `from config import ...`. When a runner imports them as
# `db.tests.<module>` (e.g. the VS Code test adapter), this folder is not on
# sys.path, so add it here: the package __init__ always runs first.
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
