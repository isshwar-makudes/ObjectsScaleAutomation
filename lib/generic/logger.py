import logging
import os
import sys
import tarfile
import time
import gzip
import shutil
from datetime import datetime

from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler


NAME = "objects_scale"

def setup_logging(abs_log_file_path, use_multi_files=True,
                  log_level="DEBUG", rotate_info=None):
  """Initializes and configures the loggers that does logging to
  console as well as in the specified file path

  Args:
    abs_log_file_path (str): The absolute log file path where log records to
                             be logged
    use_multi_files (bool): To specify whether or not to use different log 
                            files for various log levels
                            <logfile.ERROR> file will have all logs including and
                            severe than ERROR
                            <logfile.INFO> file will have all logs including and
                            severe than INFO
                            <logfile> will have all the logs corresponding to
                            various log levels permitted by means of log_level
                            option.
                            Defaults to True.
    log_level (str): The minimum log level severity that should be considered
                     for logging.
                     Defaults to 'DEBUG'
    rotate_info(dict): Contains details about log_rotation.
      Keys:
       sized_log_rotate(int) - Size in MB after which log rotates.
       timed_log_rotate(int) - Time in seconds after which log rotates.
       tar_rotated_logs(bool) - Flag to set whether rotated logs needs to be
          compressed and tarred.
  """
  log_level = getattr(logging, log_level)
  # Custom variables
  logging.marker = "-" * 60
  logging.step = 1
  logging.stage = ''

  # Create the loggers
  logging.objects_scale_logger = logging.getLogger(NAME)

  # Remove all existing handlers
  logging.objects_scale_logger.handlers = []

  # Disable the root logger
  logging.getLogger().disabled = True

  # Construct the formatter
  formatter = __get_formatter()

  # setup console handler
  console_handler = logging.StreamHandler(stream=sys.stdout)
  console_handler.setFormatter(formatter)
  logging.objects_scale_logger.addHandler(console_handler)

  # setup file handler
  __configure_file_handler(abs_log_file_path, use_multi_files, rotate_info)

  # configure logger
  logging.objects_scale_logger.setLevel(log_level)
  logging.objects_scale_logger.propagate = 0

def __configure_file_handler(abs_log_file_path, use_multi_files, rotate_info):
  formatter = __get_formatter()
  # To store all logs of various severity/log level
  if rotate_info:
    file_handler = __get_rotate_handler(abs_log_file_path, rotate_info)
  else:
    file_handler = logging.FileHandler(abs_log_file_path, 'a')
  file_handler.setFormatter(formatter)
  logging.objects_scale_logger.addHandler(file_handler)
  sys.stderr = Tee(sys.stderr, file_handler.stream)

  if use_multi_files:
    # create one for INFO and ERROR above levels
    for level_str in ("INFO", "ERROR"):
      file_path = abs_log_file_path + "."  +  level_str
      if rotate_info:
        file_handler = __get_rotate_handler(file_path, rotate_info)
      else:
        file_handler = logging.FileHandler(file_path, 'a')
      file_handler.setFormatter(formatter)
      file_handler.setLevel(getattr(logging, level_str))
      logging.objects_scale_logger.addHandler(file_handler)

def __get_rotate_handler(log_file_path, rotate_info):
  """
  Returns the log handler based on rotation attributes.
  Args:
    log_file_path(str): Log directory path.
    rotate_info(dict): Contains details about log_rotation.
      Keys:
       sized_log_rotate(int) - Size in MB after which log rotates.
       timed_log_rotate(int) - Time in seconds after which log rotates.
       tar_rotated_logs(bool) - Flag to set whether rotated logs needs to be
          compressed and tarred.
  Returns:
    logging.FileHandler(object): TimedRotatingFileHandler or
                                 RotatingFileHandler based on rotate_info.
  """
  if 'timed_log_rotate' in rotate_info:
    fh = CustomTimedRotatingFileHandler(
      filename=log_file_path, when='S',
      interval=int(rotate_info['timed_log_rotate']),
      tar_rotated_logs=rotate_info['tar_rotated_logs']
    )
  else:
    # size passed as MBs
    size = int(rotate_info['sized_log_rotate']) * 1024 * 1024
    fh = CustomRotatingFileHandler\
      (filename=log_file_path, maxBytes=size,
       tar_rotated_logs=rotate_info['tar_rotated_logs'])
  return fh

def __get_formatter():
  formatter = logging.Formatter('%(asctime)s (%(threadName)s) %(levelname)s '\
                              '[%(filename)s:%(lineno)d] : %(message)s')
  return formatter

def INFO(msg):
  logging.objects_scale_logger.info(msg)

def WARN(msg):
  logging.objects_scale_logger.warn(msg)

def ERROR(msg):
  logging.objects_scale_logger.error(msg)

def CRITICAL(msg):
  logging.objects_scale_logger.critical(msg)

def DEBUG(msg):
  logging.objects_scale_logger.debug(msg)

def LOGEXCEPTION(msg):
  logging.objects_scale_logger.exception(msg)

class CustomRotatingFileHandler(RotatingFileHandler):
  """
  This class overrides methods of RotatingFileHandler
  for custom implementation.
  """
  def __init__(self, *args, **kwargs):
    """
    Customize init to add tar_rotated_logs.
    """
    self.tar_rotated_logs = kwargs.pop('tar_rotated_logs')
    super(CustomRotatingFileHandler, self).__init__(*args, **kwargs)

  def doRollover(self):
    """
    Overrides method defined in RotatingFileHandler to overcome
    limitation of predefining backup count and add functionality
    to gzip and tar rotated logs.
    """
    if self.stream:
      self.stream.close()
      self.stream = None

    rotate_file = "%s.%s" %(self.baseFilename,
                            datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
    os.rename(self.baseFilename, rotate_file)
  
    if self.tar_rotated_logs:
      rotate_file_gz = rotate_file + ".gz"

      # gzip rotated file.
      with open(rotate_file, 'rb') as original_log, gzip.open\
                (rotate_file_gz, 'wb') as gzipped_log:
        shutil.copyfileobj(original_log, gzipped_log)
        os.remove(rotate_file)

      # Create tar file and add rotated file.
      tar_name = "%s.tar" % self.baseFilename
      with tarfile.open(tar_name, "a") as tar_handle:
        tar_handle.add(rotate_file_gz,
                       arcname=os.path.basename(rotate_file_gz))
      os.remove(rotate_file_gz)

    self.stream = self._open()

    if isinstance(sys.stderr, Tee):
      # sys.stderr needs to be re-assigned to rotated file.
      sys.stderr.stream2 = self.stream

class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
  """
    This class overrides methods of RotatingFileHandler
    for custom implementation.
  """
  def __init__(self, *args, **kwargs):
    """
    Customize init to add tar_rotated_logs.
    """
    self.tar_rotated_logs = kwargs.pop('tar_rotated_logs')
    super(CustomTimedRotatingFileHandler, self).__init__(*args, **kwargs)

  def doRollover(self):
    """
    Overrides method defined in TimedRotatingFileHandler to add
    support to gzip and tar rotated logs.
    """
    super(CustomTimedRotatingFileHandler, self).doRollover()
    if isinstance(sys.stderr, Tee):
      # sys.stderr needs to be re-assigned to rotated file.
      sys.stderr.stream2 = self.stream

    if self.tar_rotated_logs:
      files = os.listdir(os.path.dirname(self.baseFilename))
      rotated_file = ""
      for file_ in files:
        if "nutest_test.log." in file_ or "nutest_class.log." \
                in file_ and not file_.endswith(".tar"):
          rotated_file = \
            os.path.join(os.path.dirname(self.baseFilename), file_)
          break

      rotate_file = rotated_file + ".gz"

      # gzip rotated file.
      with open(rotated_file, 'rb') as original_log, gzip.open\
                (rotate_file, 'wb') as gzipped_log:
        shutil.copyfileobj(original_log, gzipped_log)
        os.remove(rotated_file)

      # Create tar file and add rotated file.
      tar_name = "%s.tar" % self.baseFilename
      with tarfile.open(tar_name, "a") as tar_handle:
        tar_handle.add(rotate_file, arcname=os.path.basename(rotate_file))
      os.remove(rotate_file)

class Tee(object):
  """This class defines an proxy to redirect stderr to both stderr and file.
  """
  def __init__(self, stream1, stream2):
    """Initialize Tee object
    Args:
      stream1(object): File handle to write to console
      stream2(object): File handle to write to file
    """
    self.stream1, self.stream2 = stream1, stream2

  def write(self, msg):
    """Writes the message to file handles
    Args:
      msg(str): Message to be written
    """
    self.stream1.write(msg)
    self.stream2.write(msg)

  def flush(self):
    """Flush the messages written so far
    """
    self.stream2.flush()
    
