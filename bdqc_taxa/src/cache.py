"""
Cache management module for bdqc_taxa.

Provides joblib-based caching for API calls with storage in the user's
platform-appropriate cache directory (appdata).

Usage:
    from bdqc_taxa.cache import memory, clear_cache, get_cache_path

    # Use memory.cache decorator on functions
    @memory.cache
    def my_cached_function(arg1: str, arg2: str) -> dict:
        ...

    # Clear all cached data
    clear_cache()

    # Get cache directory path
    path = get_cache_path()
"""

import os
import shutil
from pathlib import Path
from typing import Optional
from joblib import Memory
import platformdirs

# Cache directory in user's platform-appropriate cache location
CACHE_DIR = Path(platformdirs.user_cache_dir("bdqc_taxa"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Joblib Memory instance for caching
# verbose=0 suppresses output, set to 1 for debugging
memory = Memory(CACHE_DIR, verbose=0)


def get_cache_path() -> Path:
    """
    Return the cache directory path.

    Returns:
        Path: The path to the cache directory.
    """
    return CACHE_DIR


def get_cache_info() -> dict:
    """
    Return cache statistics and information.

    Returns:
        dict: Dictionary containing:
            - path: Cache directory path
            - size_bytes: Total size of cache in bytes
            - size_mb: Total size of cache in megabytes
            - exists: Whether cache directory exists
    """
    if not CACHE_DIR.exists():
        return {
            "path": str(CACHE_DIR),
            "size_bytes": 0,
            "size_mb": 0.0,
            "exists": False
        }

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(CACHE_DIR):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                total_size += os.path.getsize(filepath)
            except OSError:
                pass

    return {
        "path": str(CACHE_DIR),
        "size_bytes": total_size,
        "size_mb": round(total_size / (1024 * 1024), 2),
        "exists": True
    }


def clear_cache() -> None:
    """
    Clear all cached data.

    This removes all cached API responses. Use this when you need to
    refresh data from external sources or free up disk space.
    """
    memory.clear(warn=False)


def clear_cache_for_function(func) -> None:
    """
    Clear cache for a specific cached function.

    Args:
        func: The cached function whose cache should be cleared.
              This should be the original function decorated with @memory.cache

    Example:
        from bdqc_taxa.gbif import _get_cached
        clear_cache_for_function(_get_cached)
    """
    if hasattr(func, 'clear'):
        func.clear()
    else:
        raise ValueError(f"Function {func.__name__} is not a cached function or has no clear method")


def disable_cache() -> None:
    """
    Temporarily disable caching (calls will not be cached).

    Note: This affects the global memory instance. Already cached
    results will still be returned.
    """
    global memory
    memory = Memory(None, verbose=0)


def enable_cache() -> None:
    """
    Re-enable caching after it was disabled.
    """
    global memory
    memory = Memory(CACHE_DIR, verbose=0)
