import threading
import time
import json
import random

from lib.generic.utils import get_all_user_attributes_of_class, Operations
from lib.generic.locking import ReadWriteLock

GROUP_COUNT = 2 #(root node and group node)

class TestDB(object):
  VALID_OPERATIONS = get_all_user_attributes_of_class(Operations)
  IN_MEMORY_DB = {}

  def __init__(self):
    #self.top_lock = threading.Lock()
    self.group_locks = {}

  def get_lock_object(self, dotted_group):
    # with self.top_lock:
    #   for lock_group in self.group_locks.keys():
    #     if (dotted_group in lock_group or lock_group in dotted_group) and self.group_locks[lock_group].locked():
    #       raise Exception("Group lock is already acquired on %s, so locking"
    #                       " on group %s is not  possible" % (lock_group, dotted_group))
    #   if dotted_group not in self.group_locks.keys():
    #     self.group_locks[dotted_group] = threading.Lock()
    # # top_lock can be released now as its only needed when we are checking
    # # on all existing lock group names. Right now, even if some other request in
    # # some other thread gets to this, only one succeed in acquiring the lock object
    # return self.group_locks[dotted_group]
    self.group_locks.setdefault(dotted_group, ReadWriteLock())
    return self.group_locks[dotted_group]

  def set_key(self, group, key, value):
    res = self._traverse_data(group, key)
    value = json.loads(value)
    res["data"][res["last_key_part"]] = value

  def get_key(self, group, key, **kwargs):
    res = self._traverse_data(group, key)
    return res["data"][res["last_key_part"]]

  def delete_key(self, group, key, **kwargs):
    res = self._traverse_data(group, key)
    del res["data"][res["last_key_part"]]

  def add_to_value(self, group, key, value):
    res = self._traverse_data(group, key)
    res["data"][res["last_key_part"]] += value

  def increment_value(self, group, key, **kwargs):
    res = self._traverse_data(group, key)
    res["data"][res["last_key_part"]] += 1

  def pop(self, group, key, value=None):
    res = self._traverse_data(group, key)
    return res["data"].pop(res["last_key_part"], value)

  def get_whole_db(self):
    return self.IN_MEMORY_DB

  def _traverse_data(self, group, key):
    data = self.IN_MEMORY_DB
    for key_part in group.split("."):
      data.setdefault(key_part, {})
      data = data[key_part]
    keys = key.split(".")
    for key_part in keys[:-1]:
      data = data[key_part]
    return {"data": data, "last_key_part": keys[-1]}

  def perform_operation(self, dotted_group, dotted_key, operation=Operations.SET_KEY, value=None):
    if operation not in self.VALID_OPERATIONS:
      raise Exception("Invalid operation '%s' specified" % operation)
    if len(dotted_group.split(".")) != GROUP_COUNT:
      raise Exception("Invalid group %s specified. Group should have %s parts only" % (dotted_group, GROUP_COUNT))
    lock_obj = self.get_lock_object(dotted_group)
    method = getattr(self, operation.lower())
    if operation == Operations.GET_KEY:
      lock_obj.acquire_read()
    else:
      lock_obj.acquire_write()
    try:
      ret_val = method(group=dotted_group, key=dotted_key, value=value)
    finally:
      if operation == Operations.GET_KEY:
        lock_obj.release_read()
      else:
        lock_obj.release_write()
    return ret_val

if __name__ == "__main__":
  t = TestDB()
  t.set_key(group="a.b", key="c", value=json.dumps({"d":{"e": "123"}}))
  print t.get_whole_db()
  print t.get_key(group="a.b", key="c.d")
  print t.pop(group="a.b", key="c")