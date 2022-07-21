import datetime
import inspect
import json
import re
import socket
import time
import requests
from paramiko import SSHClient, AutoAddPolicy, MissingHostKeyPolicy

from lib.exceptions import SSHCommandExecutionFailed
from lib.generic.logger import INFO, ERROR, DEBUG

class Operations(object):
  SET_KEY = "SET_KEY"
  GET_KEY = "GET_KEY"
  DELETE_KEY = "DELETE_KEY"
  ADD_TO_VALUE = "ADD_TO_VALUE"
  INCREMENT_VALUE = "INCREMENT_VALUE"
  POP = "POP"

def sleep(seconds):
  for remaining in xrange(seconds, 0, -1):
    stdout.write("\r")
    stdout.write("{:2d} seconds remaining.".format(remaining))
    stdout.flush()
    sleep(1)

def get_local_ip(target="phxitcorpdcprd1.corp.nutanix.com", port=80):
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  try:
    s.connect((target, port))
    ip = s.getsockname()[0]
  finally:
    s.close()
  return ip

def convert_bool(item):
  if isinstance(item, bool):
    return item
  try:
    return json.loads(item.lower())
  except:
    pass
  if item.lower() in ['true', "y", 'yes', '1', 't']:
    return True
  if item.lower() in ['false', "n", 'no', '0', 'f']:
    return False
  raise Exception("Invalid value provided for conversion : %s" % item)

def convert_random_bool(item):
  if item == "random" or item is None:
    return item
  return convert_bool(item)

def convert_to_datetime(self, datestr):
    if isinstance(datestr, str) or isinstance(datestr, unicode):
      datestr = "%s"%(datestr.strip())
      return dateparser.parse(datestr)
    return datestr

def convert_num(num):
  try:
    return _convert_num(num)
  except Exception as err:
    ERROR("Failed to convert num %s, Err : %s"%(num, err))
    raise

#Hack to catch conversion bug
def _convert_num(num):
  if isinstance(num, str):
    if num.isdigit():
      num = int(num)
    else:
      return num
  if num == "-1" or num == -1 or num < 1000:
    return num
  magnitude = 0
  while abs(num) >= 1000:
    magnitude += 1
    num /= 1000.0
  return '%.3f%s' % (num, ['', 'K', 'M', 'B', 'T', 'QD', 'QT', 'SX', 'ST',
                            'OC', 'N', 'D'][magnitude])

def convert_to_num(size):
  if (isinstance(size, int) or isinstance(size, float) or  size.isdigit() or
      size == "-1" or size == -1):
    return int(size)
  size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
  number, unit = re.findall(r'(\d+)(\w+)', size.upper())[0]
  idx = size_name.index(unit[0].upper())
  factor = 1024 ** idx
  return int(number) * factor

def remote_exec(hostname, cmd, username="nutanix", password="nutanix/4u",
                port=22, retry=3, retry_delay=30, timeout=3600,
                background=False, debug=False, ignore_errors=None):
  if ignore_errors is None:
    ignore_errors = []
  out = err = None
  retry += 1
  while retry > 0:
    retry -= 1
    try:
      out, err = _remote_exec(hostname, cmd, username, password, port,
                                    timeout, background, debug=debug)
      return out, err
    except Exception as err:
      ERROR("Failed to execute %s on %s, Err : %s, Remaining Retry Count : %s"
            %(cmd, hostname, err, retry))
      for error in ignore_errors:
        if error in str(err):
          ERROR("Hostname : %s,Cmd : %s,Ignoring error : %s,Ignore_error : %s"
                %(hostname, cmd, err, ignore_errors))
          return out, err
      if retry > 0: time.sleep(retry_delay)
  return out, err

# TODO: This may need some more smartness for resilency
# Nice to have exit status support too
def _remote_exec(hostname, cmd, username, password, port, timeout, background,
                  debug=False):
  if debug:
    INFO("Executing :-  %s, %s, %s, %s" % (hostname, port, username, password, cmd))
  client = SSHClient()
  #client.load_system_host_keys()
  client.set_missing_host_key_policy(MissingHostKeyPolicy())
  client.set_missing_host_key_policy(AutoAddPolicy())
  client.connect(hostname, port, username, password, banner_timeout=60)
  stdin, stdout, stderr = None, None, None
  if background:
    transport = client.get_transport()
    channel = transport.open_session()
    return channel.exec_command(cmd), None
  else:
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
  out, err = stdout.read(), stderr.read()
  client.close()
  if out or not err:
    return out, err
  msg = "Failed to execute %s on %s. Error : %s (out : %s)" % (
    cmd, hostname, err, out)
  raise SSHCommandExecutionFailed(msg, out=out, err=err)

def debug_print(msg):
  print datetime.datetime.now(), "(%s)"%inspect.stack()[1][3], msg

# TODO: Use retries decorator
def copy_remote_file_to_local(self, remoteip, remotefile, localfile,
                user="root", password="nutanix/4u", retries=3, retry_delay=30):
  for i in range(retries+1):
    try:
      return self._copy_file_to_local(remoteip, remotefile, localfile, user,
                                      password)
    except Exception as err:
      ERROR("Failed to copy remote file %s:%s->%s, err : %s, retry_num  :%s"
            %(remoteip, remotefile, localfile, err, i))
      if i == retries:
        raise
    sleep(retry_delay)

def copy_local_file_to_remote(self, remoteip, localfile, remotefile,
                user="root", password="nutanix/4u", retries=3, retry_delay=30):
  for i in range(retries+1):
    try:
      return self._copy_file_to_remote(remoteip, localfile, remotefile, user,
                                      password)
    except Exception as err:
      ERROR("Failed to copy local file %s->%s:%s, err : %s, retry_num  :%s"
            %(localfile, remoteip, remotefile, err, i))
      if i == retries:
        raise
    sleep(retry_delay)

def _copy_file_to_remote(self, remoteip, localfile, remotefile, user,
                          password):
  ssh = SSHClient()
  ssh.load_system_host_keys()
  ssh.set_missing_host_key_policy(AutoAddPolicy())
  ssh.set_missing_host_key_policy(MissingHostKeyPolicy())
  ssh.connect(remoteip, username=user, password=password)
  sftp = ssh.open_sftp()
  INFO("Copying %s file to remote location : %s:%s"
        %(localfile, remoteip, remotefile))
  sftp.put(localfile, remotefile)
  sftp.close()
  ssh.close()

def _copy_file_to_local(self, remoteip, remotefile, localfile, user, passwd):
  ssh = SSHClient()
  ssh.load_system_host_keys()
  ssh.set_missing_host_key_policy(AutoAddPolicy())
  ssh.set_missing_host_key_policy(MissingHostKeyPolicy())
  ssh.connect(remoteip, username=user, password=passwd)
  sftp = ssh.open_sftp()
  INFO("Copying %s file to local location : %s:%s"
        %(remotefile, remoteip, localfile))
  sftp.get(remotefile, localfile)
  sftp.close()
  ssh.close()

def execute_http_request(url, method="GET", timeout=60, check_response=True, **kwargs):
  kwargs["timeout"] = timeout
  DEBUG("%s %s" % (method, url))
  DEBUG("Request: %s" % kwargs)
  response = requests.request(method, url, **kwargs)
  DEBUG("Response: %s" % response.content)
  if check_response:
    if not response.ok:
      raise Exception("'%s' request on url '%s' failed: %s" % (
        method, url, response.content))
  return response

def update_data(dotted_group, dotted_key, operation=None, value=None,
                timeout=60):
  data = {
    "dotted_group": dotted_group,
    "dotted_key": dotted_key,
    "value": json.dumps(value)
  }
  if operation:
    data["operation"] = operation
  url = "http://127.0.0.1:8080/execute"
  response = requests.request("POST", url, json=data, timeout=timeout)
  print("Response: %s" % response.content)
  if not response.ok:
    raise Exception("POST request on url '%s' failed: %s" % (
      url, response.content))
  return json.loads(response.content)["ret_val"]

def get_data(dotted_group, dotted_key, timeout=60):
  data = {
    "dotted_group": dotted_group,
    "dotted_key": dotted_key,
    "operation": Operations.GET_KEY
  }
  url = "http://127.0.0.1:8080/execute"
  response = requests.request("POST", url, json=data, timeout=timeout)
  print("Response: %s" % response.content)
  if not response.ok:
    raise Exception("POST  request on url '%s' failed: %s" % (
      url, response.content))
  return json.loads(response.content)["ret_val"]


def get_complete_data(timeout=60):
  url = "http://127.0.0.1:8080/data"
  response = requests.request("GET", url)
  print("Response: %s" % response.content)
  if not response.ok:
    raise Exception("GET request on url '%s' failed: %s" % (
      url, response.content))
  return json.loads(response.content)["ret_val"]

def get_random_string(length=24):
  """Generates a random alphanumeric word of specified size

  Args:
    length(int): size of the string

  Returns:
    str: Random string
  """
  import uuid
  return uuid.uuid4().hex[:length].lower()

def get_all_user_attributes_of_class(cls):
  return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith("__")]


if __name__ == "__main__":
  ans = update_data(dotted_group="a.b", dotted_key="c", value={"d":{"e": "123"}})
  print "update_data returned: %s" % ans
  print "complete data returned: "
  print get_complete_data()
  print "get_data returned: "
  print get_data(dotted_group="a.b", dotted_key="c.d")
  ans = update_data(dotted_group="a.b", dotted_key="c", operation=Operations.POP)
  print "update_data returned: %s" % ans
  print "complete data returned: "
  print get_complete_data()
