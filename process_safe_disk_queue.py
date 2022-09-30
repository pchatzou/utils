import lmdb
import tempfile
import shutil
import filelock
import os


class DiskQueue(object):
  """process safe str queue on disk."""
  encoding = 'utf-8'

  @classmethod
  def _decode(cls, my_str):
    return my_str.decode(cls.encoding)

  @classmethod
  def _encode(cls, my_str):
    return my_str.encode(cls.encoding)

  def __init__(self, fname: str, transparent=False):
    self._que_env = None
    self._max_key_size = None
    self._fname = fname
    self._transparent = transparent
    self.queue_items_put = None
    self._lock_file_path = fname + '.lock'
    if os.path.isfile(self._lock_file_path):
      os.remove(self._lock_file_path)
    self._lock_file = filelock.FileLock(self._lock_file_path, timeout=10)

  def open(self):
    self._que_env = lmdb.open(self._fname, map_size=5368709120000) # this is supposed to have space for 100 M 1kb keys
    self._max_key_size = self._que_env.max_key_size()
    self.queue_items_put = 0

  def __contains__(self, item):
    return self.get(item) is not None

  def vacuum(self):
    with tempfile.TemporaryDirectory() as tmp_dir:
      self._que_env.sync(True)
      self._que_env.copy(tmp_dir, compact=True)
      shutil.rmtree(self._fname)
      shutil.copytree(tmp_dir, self._fname)
    self.queue_items_put = 0

  def put(self, *keys: str):
    with self._lock_file:
      with self._que_env.begin(write=True) as queued:
        v = self._encode('')
        for k in keys:
          self.queue_items_put += 1
          queued.put(self._encode(k), v, dupdata=False)

  def get(self, key):
    with self._que_env.begin(write=False) as queued:
      return queued.get(self._encode(key))

  def pop(self, *keys: str):
    with self._que_env.begin(write=True) as queued:
      for k in keys:
        queued.delete(self._encode(k))

  def iterkeys(self):
    with self._que_env.begin(write=False) as queued:
      cursor = queued.cursor()
      for k, v in cursor:
        yield self._decode(k)
      cursor.close()

  def is_empty(self) -> bool:
    with self._que_env.begin(write=False) as queued:
      cursor = queued.cursor()
      ret = cursor.first()
      cursor.close()
    return not ret

  def close_handlers(self):
    self._que_env.close()

  def __del__(self):
    self.close_handlers()
    if self._transparent and os.path.exists(self._fname):
      shutil.rmtree(self._fname)
    if os.path.isfile(self._lock_file_path):
      os.remove(self._lock_file_path)

