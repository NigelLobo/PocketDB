import json
import pickle
import time
from typing import Any
import threading
import fnmatch


class PocketDBError(Exception):
    pass


class PocketDBInvalidKeyError(PocketDBError):
    pass


class PocketDBInvalidValueError(PocketDBError):
    pass


class PocketDBKeyNotFoundError(PocketDBError):
    pass


class PocketDBDiskError(PocketDBError):
    pass

# TODO add option for more efficient implementation
# TODO add ttl list option


class PocketDB:
    _AUTO_SAVE_INTERVAL_SECS = 60

    def __init__(self, name: str = 'pocketdb'):
        self.name = name
        self.default_filename = f'{self.name}.pdb'
        self._data = {}
        self._ttl = {}  # lazy evaluation

        self._stats = {
            'gets': 0,
            'sets': 0,
            'deletes': 0,
            'hits': 0,
            'misses': 0,
            'expired': 0
        }

        self._lock = threading.RLock()
        self._is_running = True
        self._auto_save_thread = None
        self._begin_auto_save()

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        self._validate_key(key)
        self._validate_value(value)

        with self._lock:
            self._prune_expired()
            self._data[key] = value

            if isinstance(ttl, int):
                if ttl <= 0:
                    raise PocketDBInvalidValueError(
                        'Expiry cannot be in the past!')
                self._ttl[key] = time.time() + ttl
            elif key in self._ttl:
                # discard unused ttl
                del self._ttl[key]

        return True

    def get(self, key: str, default_value: Any = None) -> Any:
        self._validate_key(key)

        with self._lock:
            self._prune_expired()

            if key in self._data:
                self._stats['gets'] += 1
                self._stats['hits'] += 1
                return self._data[key]
            else:
                self._stats['gets'] += 1
                self._stats['misses'] += 1
                if default_value is not None:
                    return default_value
                raise PocketDBKeyNotFoundError(f"Key '{key}' not found!")

    def delete(self, key: str) -> bool:
        self._validate_key(key)

        with self._lock:
            self._prune_expired()

            if key in self._data:
                del self._data[key]
                if key in self._ttl:
                    del self._ttl[key]
                self._stats['deletes'] += 1
                return True
            return False

    def exists(self, key: str) -> bool:
        self._validate_key(key)

        with self._lock:
            self._prune_expired()
            return key in self._data

    def _validate_key(self, key: str):
        if not isinstance(key, str) or not key.strip():
            raise PocketDBInvalidKeyError

    def _validate_value(self, value: Any):
        # doesn't really matter what it is...but it should be serializable
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            raise PocketDBInvalidValueError(
                "Value must be JSON serializable!")

    def quit(self):
        self._is_running = False
        self.save_to_disk()
        print('PocketDB shut down.')

    def save_to_disk(self, filename: str | None = None):
        # save current DB as a backup
        if filename is None:
            filename = self.default_filename

        try:
            with self._lock:
                self._prune_expired()
                backup = {
                    'data': self._data,
                    'ttl': self._ttl,
                    'stats': self._stats
                }

                with open(filename, 'wb') as file:
                    pickle.dump(backup, file)

                return True
        except Exception as err:
            raise PocketDBDiskError(
                f'Could not save to disk: {err}')

    def load_from_disk(self, filename: str | None = None) -> bool:
        if filename is None:
            filename = self.default_filename

        try:
            with open(filename, 'rb') as file:
                backup = pickle.load(file)

                with self._lock:
                    self._data = backup.get('data')
                    self._ttl = backup.get('ttl', {})
                    self._stats = backup.get('stats', self._stats)

                    self._prune_expired()
                    return True

        except Exception as err:
            raise PocketDBDiskError(f'Could not load from disk: {err}')

    def _begin_auto_save(self):
        def _worker():
            while self._is_running:
                time.sleep(self._AUTO_SAVE_INTERVAL_SECS)
                try:
                    self.save_to_disk()
                except Exception as err:
                    print(f'There was an error while saving: {err}')

        if self._auto_save_thread is None:
            self._auto_save_thread = threading.Thread(
                target=_worker, daemon=True)
            self._auto_save_thread.start()

    def _prune_expired(self):
        # lock should be provided by caller
        system_time = time.time()
        expired_keys = []

        for key, expiry in self._ttl.items():
            if system_time > expiry:
                expired_keys.append(key)

        for key in expired_keys:
            del self._data[key]
            del self._ttl[key]
            self._stats['expired'] += 1

    def size(self) -> int:
        with self._lock:
            self._prune_expired()
            return len(self._data)

    def values(self) -> list[Any]:
        """
        Get all values in the database.

        Returns:
            List of all values
        """
        with self._lock:
            self._prune_expired()
            return list(self._data.values())

    def keys(self, pattern: str = "*") -> list[str]:
       # returns list of keys matching some wildcard
        with self._lock:
            self._prune_expired()

            if pattern == "*":
                return list(self._data.keys())

            # Simple wildcard matching
            if "*" in pattern:
                return [key for key in self._data.keys() if fnmatch.fnmatch(key, pattern)]
            else:
                return [key for key in self._data.keys() if key == pattern]

    def values(self) -> list[Any]:

        with self._lock:
            self._prune_expired()
            return list(self._data.values())

    def items(self) -> list[tuple]:

        with self._lock:
            self._prune_expired()
            return list(self._data.items())

    def reset(self):
        with self._lock:
            self._data = {}
            self._ttl = {}

    def stats(self) -> dict[str, Any]:
        with self._lock:
            self._prune_expired()

            hit_rate = 0
            if self._stats['gets'] > 0:
                hit_rate = self._stats['hits'] / self._stats['gets']

            return {
                'size': len(self._data),
                'ttl_keys': len(self._ttl),
                'gets': self._stats['gets'],
                'sets': self._stats['sets'],
                'deletes': self._stats['deletes'],
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'expired': self._stats['expired'],
                'hit_rate': hit_rate,
            }
