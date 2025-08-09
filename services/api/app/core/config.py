import os, time, threading, yaml
from typing import Any, Dict

_lock = threading.RLock()
_cache: Dict[str, Any] | None = None
_cache_mtime: float | None = None
_cfg_path: str | None = None

def _read_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def configure(path: str) -> None:
    global _cfg_path
    with _lock:
        _cfg_path = path
        _reload_locked()

def _reload_locked() -> None:
    global _cache, _cache_mtime
    if _cfg_path is None:
        _cache, _cache_mtime = {}, None
        return
    mtime = os.path.getmtime(_cfg_path) if os.path.exists(_cfg_path) else None
    if mtime is not None and _cache_mtime is not None and mtime == _cache_mtime:
        return
    _cache = _read_yaml(_cfg_path)
    _cache_mtime = mtime

def get_config() -> Dict[str, Any]:
    with _lock:
        _reload_locked()
        return _cache or {}

def reload_config() -> Dict[str, Any]:
    with _lock:
        _reload_locked()
        return _cache or {}
