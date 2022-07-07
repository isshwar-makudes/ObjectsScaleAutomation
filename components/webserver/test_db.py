IN_MEMORY_DB = {}

class TestDB(object):
  def add_key_value(key, value):
    global IN_MEMORY_DB
    keys = key.split(".")
    data = IN_MEMORY_DB
    for key in keys[:-1]:
      data[key] = {}
      data = data[key]
    data[keys[-1]] = value

    
