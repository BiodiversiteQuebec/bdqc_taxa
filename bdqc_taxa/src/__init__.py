from .__about__ import (
    __author__,
    __copyright__,
    __email__,
    __summary__,
    __title__,
    __uri__,
    __version__,
)

import os
import importlib
import sys

from . import bryoquel
from . import wikidata
from . import gbif
from . import vernacular
from . import taxa_ref
from . import eliso
from . import cdpnq

__all__ = [
    "__title__",
    "__summary__",
    "__uri__",
    "__version__",
    "__author__",
    "__email__",
    "__copyright__",
    "bryoquel",
    "wikidata",
    "gbif",
    "vernacular",
    "taxa_ref",
    "eliso",
    "cdpnq",
]