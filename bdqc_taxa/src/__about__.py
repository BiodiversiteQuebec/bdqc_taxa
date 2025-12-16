from collections import OrderedDict
__all__ = [
    "__title__",
    "__summary__",
    "__uri__",
    "__version__",
    "__author__",
    "__email__",
    "__copyright__",
]

__title__ = "bdqc_taxa"
__summary__ = "`BIOQC-taxa` is a python package that interface with *Biodiversité Québec*'s database to query reference taxa sources, parse their return and generate records."
__uri__ = "https://github.com/ReseauBiodiversiteQuebec/bdqc-taxa"

_version = OrderedDict(
    major = 0,
    minor = 19,
    patch = 5
)
__version__ = ".".join([str(v) for v in _version.values()])

__author__ = "Vincent Beauregard"
__email__ = "vincent.beauregard@usherbrooke.ca"

__copyright__ = "2021-2024 %s" % __author__
