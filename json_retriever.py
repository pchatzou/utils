import re
import json

class DictMatch(object):

  def __init__(self, keys, limit=2000):
    """Make a compiled pattern that will match a json object in a string
    that contains all regexes for given keys, that are scrpit params. Order of keys is indifferent.
    Use with more than one key.
    Args:
      keys (tuple): regex strings for keys
      limit (int): how many iterations to allow to find closing brackets.
    """
    if len(keys) < 2:
      raise TypeError('Should be used for more than one key')
    remove_word = r'\W+{'
    self._remove_word = re.compile(remove_word)
    self._reverse_remove_word = re.compile(r'{\W+')
    ret = r'{"[^{]*'
    self._keys = []
    for key in keys:
      ret += r'(?=.*' + key + ')'
      self._keys.append(re.compile(key))
    ret += r'.+?}'
    self.regex = re.compile(ret)
    self.limit = limit
    self._cursor = 0
    self._cnt = 0
    self._prev = None

  def dict_iter(self, my_val):
    self._cursor = 0
    while True:
      try:
        ret = self.dict_from_regex(my_val[self._cursor:])
      except TypeError:
        break
      if not ret:
        break
      yield ret

  def _balance_brackets(self, my_val, my_str, my_start, my_end):
    openings = my_str.count('{')
    closings = my_str.count('}')
    while openings == closings and self._cnt < self.limit:
      my_start = my_val.rfind('{', 0, my_start - 1)
      my_str = my_val[my_start: my_end + 1]
      openings = my_str.count('{')
      closings = my_str.count('}')
      self._cnt += 1
    while openings > closings and self._cnt < self.limit:
      my_end = my_val.find('}', my_end + 1)
      my_str = my_val[my_start: my_end + 1]
      openings = my_str.count('{')
      closings = my_str.count('}')
      self._cnt += 1
    while openings < closings and self._cnt < self.limit:
      my_start = my_val.rfind('{', 0, my_start - 1)
      my_str = my_val[my_start: my_end + 1]
      openings = my_str.count('{')
      closings = my_str.count('}')
      self._cnt += 1
    return my_str, my_start, my_end

  def _truncate_for_speedup(self, my_val):
    truncate_starts = []
    for key in self._keys:
      key_found = key.search(my_val)
      if key_found:
        truncate_starts.append(key_found.start())
    if len(truncate_starts) == len(self._keys):
      last_key = max(truncate_starts)
      start_re = self._reverse_remove_word.search(my_val[last_key::-1])
      end_re = re.search('}', my_val[max(truncate_starts):])
      if start_re and end_re:
        return last_key - start_re.end(), last_key + end_re.end()
      else:
        return -1, -1
    else:
      return -1, -1

  def _subdict_from_big(self, my_dict):
    if type(my_dict) is dict:
      my_str = json.dumps(my_dict).encode('utf-8')
      if self._keys[0].search(my_str):
        ret = True
        for mk in self._keys[1:]:
          if not mk.search(my_str):
            ret = False
            break
        if ret:
          self._prev = my_dict
      for key, value in my_dict.items():
        if self._keys[0].search(json.dumps(value).encode('utf-8')):
          self._subdict_from_big(value)
    elif type(my_dict) is list:
      for item in my_dict:
        self._subdict_from_big(item)

  def dict_from_regex(self, my_val):
    """When a match, make sure closing brackets are correct."""
    start_at, end_at = self._truncate_for_speedup(my_val)
    my_match = self.regex.search(my_val[start_at:end_at])
    while my_match is None and end_at > 0:
      start_tmp, end_tmp = self._truncate_for_speedup(my_val[end_at:])
      if start_tmp < 0 or end_tmp < 0:
        break
      start_at += start_tmp
      end_at += end_tmp
      my_match = self.regex.search(my_val[start_at:end_at])
    ret = {}
    if my_match:
      my_str_init = my_match.group()
      my_str = self._remove_word.split(my_str_init, 1)
      if len(my_str) > 1:
        my_str = my_str[1]
      else:
        my_str = my_str_init
      my_start = start_at + my_match.start() + len(my_str_init) - len(my_str) - 1
      my_end = start_at + my_match.end()
      my_str, my_start, my_end = self._balance_brackets(my_val, my_str, my_start, my_end)
      while self._cnt < self.limit:
        try:
          ret = json.loads(my_str)
          break
        except ValueError:
          my_str, my_start, my_end = self._balance_brackets(my_val, my_str, my_start, my_end)
      if not ret:
        raise TypeError
      self._cursor += my_end + 1
    self._cnt = 0
    self._prev = {}
    self._subdict_from_big(ret)
    return self._prev
