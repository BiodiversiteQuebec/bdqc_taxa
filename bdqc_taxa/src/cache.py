"""
Cache management module for bdqc_taxa.

Provides joblib-based caching for API calls with storage in the user's
platform-appropriate cache directory (appdata).

Usage:
    from bdqc_taxa.cache import cache, clear_cache, get_cache_path

    # Use cache decorator on functions
    @cache.memoize()  # Cache without expiration
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
import platformdirs
from diskcache import Cache
import functools

# Cache directory in user's platform-appropriate cache location
CACHE_DIR = Path(platformdirs.user_cache_dir("bdqc_taxa"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Disk cache instance for persistent, durable caching
cache = Cache(directory=str(CACHE_DIR))


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
    cache.clear()


def clear_cache_for_function(func) -> None:
    """
    Clear cache for a specific cached function.

    Args:
        func: The cached function whose cache should be cleared.
              This should be the original function decorated with @cache.memoize().

    Example:
        from bdqc_taxa.gbif import _get_cached
        clear_cache_for_function(_get_cached)
    """
    if hasattr(func, 'cache'):
        func.cache.clear()
    else:
        raise ValueError("The provided function is not cached with @cache.memoize().")